use crate::kv_store::{KVStore, RocksDB};
use crate::types::structs::{
    BalanceRpcResult, DeployProcessed, NodeConfig, StateRootHashRpcResult,
};
use anyhow::Error;
use casper_client::{get_balance, get_state_root_hash, Error as ClientError};
use casper_types::account::AccountHash;
use casper_types::{ExecutionResult, Transfer, Transform, URef, U512};
use std::path::Path;
use std::str::FromStr;
use tracing::info;

pub struct BalanceIndexer {
    store: RocksDB,
    node_rpc_url: String,
    state_root_hash: String,
}

enum TransferDirection {
    Sent,
    Received,
}

impl BalanceIndexer {
    pub async fn new(store_path: &Path, node: &NodeConfig) -> Result<Self, Error> {
        let formatted_url = format!("http://{}:{}", node.ip_address, node.rpc_port);
        match store_path.to_str() {
            None => Err(Error::msg("Failed to parse store path provided")),
            Some(path) => match RocksDB::new(path) {
                Ok(store) => {
                    store.save("health-check", "healthy")?;
                    Ok(BalanceIndexer {
                        store,
                        node_rpc_url: formatted_url.clone(),
                        state_root_hash: retrieve_state_root_hash(&formatted_url).await?,
                    })
                }
                Err(e) => Err(Error::msg(e.to_string())),
            },
        }
    }

    pub async fn commit_balances(&mut self, transfer: &Transfer) -> Result<(), Error> {
        let from_hex = hex::encode(transfer.from.value());
        let amount = transfer.amount;

        let truncated_from = truncate_long_string(&from_hex);
        let truncated_to = match transfer.to {
            None => None,
            Some(to_hash) => Some(truncate_long_string(hex::encode(to_hash.value()).as_str())),
        };

        info!(
            message = "New transfer:",
            from = truncated_from.as_str(),
            to = truncated_to
                .unwrap_or(String::from("Not provided"))
                .as_str(),
            amount_motes = amount.to_string().as_str(),
            amount_cspr = motes_to_cspr(amount).to_string().as_str()
        );

        self.save_or_update_balance(
            &transfer.source,
            Some(transfer.from),
            amount,
            TransferDirection::Sent,
        )
        .await?;
        self.save_or_update_balance(
            &transfer.target,
            transfer.to,
            amount,
            TransferDirection::Received,
        )
        .await?;

        Ok(())
    }

    async fn save_or_update_balance(
        &mut self,
        purse_uref: &URef,
        account_hash: Option<AccountHash>,
        amount: U512,
        direction: TransferDirection,
    ) -> Result<(), Error> {
        match self.store.find(&purse_uref.to_string()) {
            Ok(res) => match res {
                None => {
                    match account_hash {
                        None => {
                            // From will always be provided so we don't need to handle it
                            info!("\tNew purse: <To field (AccountHash) not provided in transfer>")
                        }
                        Some(hash) => {
                            let hash_hex = hex::encode(&hash.value());
                            let key = format!("purse-of-{}", &hash_hex);
                            info!("New Mapping: {} to {}", key, &purse_uref.to_formatted_string());
                            self.store.save(&key, &purse_uref.to_formatted_string()).unwrap();
                            info!(
                                "\tNew account: {}",
                                truncate_long_string(&hash_hex)
                            )
                        },
                    }
                    let balance_from_node = self.retrieve_balance(purse_uref).await?;
                    let new_balance;
                    match balance_from_node {
                        // This assumes it's an account that has only just been initialised by the transfer.
                        None => new_balance = amount,
                        Some(balance) => {
                            new_balance = match direction {
                                TransferDirection::Sent => balance - amount,
                                TransferDirection::Received => balance + amount,
                            };
                        }
                    }
                    self.store
                        .save(&purse_uref.to_formatted_string(), &new_balance.to_string())?;
                    info!("\tCommitted balance: {} CSPR", motes_to_cspr(new_balance));
                    Ok(())
                }
                Some(x) => {
                    match account_hash {
                        None => {
                            // From will always be provided so we don't need to handle it
                            info!("\tKnown account: <To field (AccountHash) not provided in transfer>")
                        }
                        Some(hash) => info!(
                            "\tKnown account: {}",
                            truncate_long_string(hex::encode(&hash.value()).as_str())
                        ),
                    }
                    let balance_from_store = U512::from_str_radix(&x, 10)?;
                    let new_balance = match direction {
                        TransferDirection::Sent => balance_from_store - amount,
                        TransferDirection::Received => balance_from_store + amount,
                    };
                    self.store
                        .save(&purse_uref.to_formatted_string(), &new_balance.to_string())?;
                    info!(
                        "\tUpdating balance from {} CSPR -> {} CSPR",
                        motes_to_cspr(balance_from_store),
                        motes_to_cspr(new_balance)
                    );
                    Ok(())
                }
            },
            Err(err) => Err(Error::from(err)),
        }
    }

    async fn retrieve_balance(&self, purse_uref: &URef) -> Result<Option<U512>, Error> {
        match get_balance(
            "",
            &self.node_rpc_url,
            0,
            &self.state_root_hash,
            &purse_uref.to_formatted_string(),
        )
        .await
        {
            Err(rpc_err) => match rpc_err {
                ClientError::ResponseIsError(err) => {
                    if err.code == -32007 {
                        Ok(None)
                    } else {
                        Err(Error::from(err))
                    }
                }
                err => Err(Error::from(err)),
            },
            Ok(rpc_response) => match rpc_response.get_result() {
                None => Err(Error::msg("Empty response from RPC")),
                Some(value) => match serde_json::from_value::<BalanceRpcResult>(value.to_owned()) {
                    Err(serde_err) => Err(Error::from(serde_err)),
                    Ok(result) => {
                        // Parse balance into U512 if possible
                        match U512::from_str(&result.balance_value) {
                            Ok(balance) => Ok(Some(balance)),
                            Err(err) => Err(Error::from(err)),
                        }
                    }
                },
            },
        }
    }
}

pub fn parse_transfers_from_deploy(deploy: &DeployProcessed) -> Option<Vec<Transfer>> {
    return match &*deploy.execution_result {
        ExecutionResult::Failure { .. } => None,
        ExecutionResult::Success {
            effect, transfers, ..
        } => {
            // This can be included again once I improve the test suite.
            //
            if transfers.is_empty() {
                return None;
            }
            let transfers: Vec<Transfer> = effect
                .transforms
                .iter()
                .filter_map(|transform_entry| match transform_entry.transform {
                    Transform::WriteTransfer(transfer) => Some(Transfer::from(transfer)),
                    _ => None,
                })
                .collect();
            if transfers.get(0).is_none() {
                return None;
            }
            Some(transfers)
        }
    };
}

async fn retrieve_state_root_hash(node_rpc: &str) -> Result<String, Error> {
    match get_state_root_hash("", node_rpc, 0, "").await {
        Err(rpc_err) => Err(Error::from(rpc_err)),
        Ok(rpc_response) => match rpc_response.get_result() {
            None => Err(Error::msg("Empty response from RPC")),
            Some(value) => {
                match serde_json::from_value::<StateRootHashRpcResult>(value.to_owned()) {
                    Err(serde_err) => Err(Error::from(serde_err)),
                    Ok(result) => Ok(result.state_root_hash),
                }
            }
        },
    }
}

fn truncate_long_string(long_string: &str) -> String {
    let length = long_string.len();
    format!("{}...{}", &long_string[..10], &long_string[length - 6..])
}

// I think this is rounding incorrectly, integer division discarding the decimal
fn motes_to_cspr(motes: U512) -> U512 {
    motes / 1000000000
}

#[cfg(test)]
mod tests {
    use crate::balance_indexer::{parse_transfers_from_deploy, BalanceIndexer};
    use crate::types::structs::{DeployProcessed, New};
    use crate::{read_config, Config, Network};
    use std::path::Path;
    use anyhow::Error;
    use crate::kv_store::KVStore;

    struct TestState {
        d_w_transfers: DeployProcessed,
        d_wo_transfers: DeployProcessed,
        d_wp_transfers: DeployProcessed,
    }

    fn before_each() -> TestState {
        let deploy_with_transfers: DeployProcessed = DeployProcessed::new_with_transfers();
        let deploy_without_transfers: DeployProcessed = DeployProcessed::new_without_transfers();
        let deploy_with_populated: DeployProcessed = DeployProcessed::new_populated_transfers();

        return TestState {
            d_w_transfers: deploy_with_transfers,
            d_wo_transfers: deploy_without_transfers,
            d_wp_transfers: deploy_with_populated,
        };
    }

    #[test]
    fn parse_transfers_should_work() {
        let state = before_each();

        assert!(parse_transfers_from_deploy(&state.d_wo_transfers).is_none());

        let parsed_transfers = parse_transfers_from_deploy(&state.d_w_transfers);
        match parsed_transfers {
            None => {
                panic!("Should have parsed transfers, instead got None");
            }
            Some(transfers) => {
                assert_eq!(transfers.len(), 2);
                assert!(transfers.get(0).is_some());
                assert!(transfers.get(1).is_some());
            }
        }
    }

    #[tokio::test]
    async fn save_and_find_should_work() {
        let store_path = String::from("./test/kv_store_1");
        let config: Config = read_config("config.toml").unwrap();

        let node_config = match config.connection.network {
            Network::Mainnet => config.connection.node.mainnet,
            Network::Testnet => config.connection.node.testnet,
            Network::Local => config.connection.node.local,
        };

        // Delete old RocksDB instance if it exists
        std::fs::remove_dir_all(&store_path).unwrap_or(());

        let mut t_indexer = BalanceIndexer::new(Path::new(&store_path), &node_config)
            .await
            .unwrap();

        t_indexer.store.save("test", "hello").unwrap();

        let found = t_indexer.store.find("test").unwrap();
        assert!(found.is_some());
        assert_eq!(found.unwrap(), "hello");
    }

    #[tokio::test]
    async fn should_save_populated_transfers() {
        let store_path = String::from("./test/kv_store_2");
        let state = before_each();
        let config: Config = read_config("config.toml").unwrap();

        let node_config = match config.connection.network {
            Network::Mainnet => config.connection.node.mainnet,
            Network::Testnet => config.connection.node.testnet,
            Network::Local => config.connection.node.local,
        };

        // Delete old RocksDB instance if it exists
        std::fs::remove_dir_all(&store_path).unwrap_or(());

        let mut t_indexer = BalanceIndexer::new(Path::new(&store_path), &node_config)
            .await
            .unwrap();

        let transfers_with_to =
            parse_transfers_from_deploy(&state.d_wp_transfers).expect("parse_transfers failed");

        for transfer in transfers_with_to {
            let check = t_indexer.commit_balances(&transfer).await;
            assert!(check.is_ok());
            let source_balance = t_indexer.retrieve_balance(&transfer.source).await;
            assert!(source_balance.is_ok());
            assert!(source_balance.unwrap().is_some());
            let to_balance = t_indexer.retrieve_balance(&transfer.target).await;
            assert_eq!(to_balance.unwrap().unwrap(), transfer.amount);
        }


    }

    #[tokio::test]
    async fn should_save_default_transfers() {
        let store_path = String::from("./test/kv_store_3");
        let state = before_each();
        let config: Config = read_config("config.toml").unwrap();

        let node_config = match config.connection.network {
            Network::Mainnet => config.connection.node.mainnet,
            Network::Testnet => config.connection.node.testnet,
            Network::Local => config.connection.node.local,
        };

        // Delete old RocksDB instance if it exists
        std::fs::remove_dir_all(&store_path).unwrap_or(());

        let mut t_indexer = BalanceIndexer::new(Path::new(&store_path), &node_config)
            .await
            .unwrap();

        let transfers_without_to =
            parse_transfers_from_deploy(&state.d_w_transfers).expect("parse_transfers failed");
        for transfer in transfers_without_to {
            assert!(transfer.to.is_none());
            let check = t_indexer.commit_balances(&transfer).await;
            assert!(check.is_ok());
        }
    }

}
