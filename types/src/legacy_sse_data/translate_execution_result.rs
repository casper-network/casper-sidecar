use casper_types::{
    addressable_entity::NamedKeys,
    execution::{
        execution_result_v1::{ExecutionEffect, NamedKey, TransformKindV1, TransformV1},
        Effects, ExecutionResultV1, ExecutionResultV2, TransformV2,
    },
    StoredValue,
};

pub fn build_default_execution_result_translator(
) -> DefaultExecutionResultV2Translator<DefaultExecutionEffectsTranslator> {
    DefaultExecutionResultV2Translator {
        effects_translator: DefaultExecutionEffectsTranslator,
    }
}

pub trait ExecutionResultV2Translator {
    fn translate(&self, result: &ExecutionResultV2) -> Option<ExecutionResultV1>;
}

pub trait ExecutionEffectsTranslator {
    fn translate(&self, effects: &Effects) -> Option<ExecutionEffect>;
}

pub struct DefaultExecutionResultV2Translator<EET>
where
    EET: ExecutionEffectsTranslator,
{
    effects_translator: EET,
}

impl<EET> ExecutionResultV2Translator for DefaultExecutionResultV2Translator<EET>
where
    EET: ExecutionEffectsTranslator,
{
    fn translate(&self, result: &ExecutionResultV2) -> Option<ExecutionResultV1> {
        let maybe_effects = self.effects_translator.translate(&result.effects);
        if let Some(effect) = maybe_effects {
            if let Some(err_msg) = &result.error_message {
                Some(ExecutionResultV1::Failure {
                    effect,
                    transfers: vec![],
                    cost: result.cost,
                    error_message: err_msg.to_string(),
                })
            } else {
                Some(ExecutionResultV1::Success {
                    effect,
                    transfers: vec![],
                    cost: result.cost,
                })
            }
        } else {
            None
        }
    }
}

pub struct DefaultExecutionEffectsTranslator;

impl ExecutionEffectsTranslator for DefaultExecutionEffectsTranslator {
    fn translate(&self, effects: &Effects) -> Option<ExecutionEffect> {
        let mut transforms: Vec<TransformV1> = Vec::new();
        for ex_ef in effects.transforms() {
            let key = *ex_ef.key();
            let maybe_transform_kind = map_transform_v2(ex_ef);
            if let Some(transform_kind) = maybe_transform_kind {
                let transform = TransformV1 {
                    key: key.to_string(),
                    transform: transform_kind,
                };
                transforms.push(transform);
            } else {
                // If we stumble on a transform we can't translate, we should clear all of them
                // so that the user won't get a partial view of the effects.
                transforms.clear();
                break;
            }
        }
        Some(ExecutionEffect {
            // Operations will be empty since we can't translate them (no V2 entity has a corresponding entity in V1).
            operations: vec![],
            transforms,
        })
    }
}

fn map_transform_v2(ex_ef: &TransformV2) -> Option<TransformKindV1> {
    let maybe_transform_kind = match ex_ef.kind() {
        casper_types::execution::TransformKindV2::Identity => Some(TransformKindV1::Identity),
        casper_types::execution::TransformKindV2::Write(stored_value) => {
            maybe_tanslate_stored_value(stored_value)
        }
        casper_types::execution::TransformKindV2::AddInt32(v) => {
            Some(TransformKindV1::AddInt32(*v))
        }
        casper_types::execution::TransformKindV2::AddUInt64(v) => {
            Some(TransformKindV1::AddUInt64(*v))
        }
        casper_types::execution::TransformKindV2::AddUInt128(v) => {
            Some(TransformKindV1::AddUInt128(*v))
        }
        casper_types::execution::TransformKindV2::AddUInt256(v) => {
            Some(TransformKindV1::AddUInt256(*v))
        }
        casper_types::execution::TransformKindV2::AddUInt512(v) => {
            Some(TransformKindV1::AddUInt512(*v))
        }
        casper_types::execution::TransformKindV2::AddKeys(keys) => handle_named_keys(keys),
        casper_types::execution::TransformKindV2::Prune(key) => Some(TransformKindV1::Prune(*key)),
        casper_types::execution::TransformKindV2::Failure(err) => {
            Some(TransformKindV1::Failure(err.to_string()))
        }
    };
    maybe_transform_kind
}

fn handle_named_keys(keys: &NamedKeys) -> Option<TransformKindV1> {
    let mut named_keys = vec![];
    for (name, key) in keys.iter() {
        let named_key = NamedKey {
            name: name.to_string(),
            key: key.to_string(),
        };
        named_keys.push(named_key);
    }
    Some(TransformKindV1::AddKeys(named_keys))
}

fn maybe_tanslate_stored_value(stored_value: &StoredValue) -> Option<TransformKindV1> {
    //TODO stored_value this shouldn't be a reference. we should take ownership and reassign to V1 enum to avoid potentially expensive clones.
    match stored_value {
        StoredValue::CLValue(cl_value) => Some(TransformKindV1::WriteCLValue(cl_value.clone())),
        StoredValue::Account(acc) => Some(TransformKindV1::WriteAccount(acc.account_hash())),
        StoredValue::ContractWasm(_) => Some(TransformKindV1::WriteContractWasm),
        StoredValue::Contract(_) => Some(TransformKindV1::WriteContract),
        StoredValue::ContractPackage(_) => Some(TransformKindV1::WriteContractPackage),
        StoredValue::LegacyTransfer(transfer) => {
            Some(TransformKindV1::WriteTransfer(transfer.clone()))
        }
        StoredValue::DeployInfo(deploy_info) => {
            Some(TransformKindV1::WriteDeployInfo(deploy_info.clone()))
        }
        StoredValue::EraInfo(era_info) => Some(TransformKindV1::WriteEraInfo(era_info.clone())),
        StoredValue::Bid(bid) => Some(TransformKindV1::WriteBid(bid.clone())),
        StoredValue::Withdraw(withdraw) => Some(TransformKindV1::WriteWithdraw(withdraw.clone())),
        StoredValue::Unbonding(p) => Some(TransformKindV1::WriteUnbonding(p.clone())),
        StoredValue::NamedKey(named_key) => {
            let key_res = named_key.get_key();
            let name_res = named_key.get_name();
            if let (Ok(key), Ok(name)) = (key_res, name_res) {
                Some(TransformKindV1::AddKeys(vec![NamedKey {
                    name: name.to_string(),
                    key: key.to_string(),
                }]))
            } else {
                None
            }
        }
        // following variuant will not be understood by old clients since they are introduced in 2.x
        StoredValue::AddressableEntity(_) => None,
        StoredValue::BidKind(_) => None,
        StoredValue::Package(_) => None,
        StoredValue::ByteCode(_) => None,
        StoredValue::MessageTopic(_) => None,
        StoredValue::Message(_) => None,
        StoredValue::Reservation(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::maybe_tanslate_stored_value;
    use casper_types::{
        account::{AccountHash, ActionThresholds, AssociatedKeys, Weight},
        addressable_entity::NamedKeys,
        contracts::{ContractPackage, ContractPackageStatus, ContractVersions, DisabledVersions},
        execution::execution_result_v1::TransformKindV1,
        AccessRights, Account, CLValue, Groups, StoredValue, URef,
    };

    #[test]
    fn maybe_tanslate_stored_value_should_translate_values() {
        let stored_value = StoredValue::CLValue(CLValue::from_t(1).unwrap());
        assert_eq!(
            Some(TransformKindV1::WriteCLValue(CLValue::from_t(1).unwrap())),
            maybe_tanslate_stored_value(&stored_value)
        );

        let account = random_account();
        let stored_value = StoredValue::Account(account);
        assert_eq!(
            Some(TransformKindV1::WriteAccount(AccountHash::new([9u8; 32]))),
            maybe_tanslate_stored_value(&stored_value)
        );

        let contract_package = random_contract_package();
        let stored_value = StoredValue::ContractPackage(contract_package);
        assert_eq!(
            Some(TransformKindV1::WriteContractPackage),
            maybe_tanslate_stored_value(&stored_value)
        );
        //TODO wrtite tests for rest of cases
    }

    fn random_account() -> Account {
        let account_hash = AccountHash::new([9u8; 32]);
        let action_thresholds = ActionThresholds {
            deployment: Weight::new(8),
            key_management: Weight::new(11),
        };
        Account::new(
            account_hash,
            NamedKeys::default(),
            URef::new([43; 32], AccessRights::READ_ADD_WRITE),
            AssociatedKeys::default(),
            action_thresholds,
        )
    }

    fn random_contract_package() -> ContractPackage {
        ContractPackage::new(
            URef::new([0; 32], AccessRights::NONE),
            ContractVersions::default(),
            DisabledVersions::default(),
            Groups::default(),
            ContractPackageStatus::default(),
        )
    }
}
