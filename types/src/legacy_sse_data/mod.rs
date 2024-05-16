use self::{
    translate_block_added::{build_default_block_added_translator, BlockAddedTranslator},
    translate_execution_result::{
        build_default_execution_result_translator, ExecutionResultV2Translator,
    },
};
use crate::sse_data::SseData;
use casper_types::{
    execution::{ExecutionResult, ExecutionResultV1},
    BlockHash, Deploy, DeployHash, EraId, FinalitySignature, FinalitySignatureV1,
    FinalitySignatureV2, InitiatorAddr, ProtocolVersion, PublicKey, Signature, TimeDiff, Timestamp,
    Transaction, TransactionHash,
};
use serde::{Deserialize, Serialize};

#[cfg(test)]
mod fixtures;
mod structs;
mod translate_block_added;
mod translate_deploy_hashes;
mod translate_execution_result;

#[derive(Clone, Eq, PartialEq, Serialize, Deserialize, Debug)]
pub enum LegacySseData {
    ApiVersion(ProtocolVersion),
    DeployAccepted(Deploy),
    DeployProcessed {
        deploy_hash: Box<DeployHash>,
        account: Box<PublicKey>,
        timestamp: Timestamp,
        ttl: TimeDiff,
        dependencies: Vec<DeployHash>,
        block_hash: Box<BlockHash>,
        execution_result: Box<ExecutionResultV1>,
    },
    DeployExpired {
        deploy_hash: DeployHash,
    },
    BlockAdded {
        block_hash: BlockHash,
        block: structs::BlockV1,
    },
    Fault {
        era_id: EraId,
        public_key: PublicKey,
        timestamp: Timestamp,
    },
    FinalitySignature(LegacyFinalitySignature),
}

#[derive(Clone, Eq, PartialEq, Serialize, Deserialize, Debug)]
pub struct LegacyFinalitySignature {
    block_hash: BlockHash,
    era_id: EraId,
    signature: Signature,
    public_key: PublicKey,
}

impl LegacyFinalitySignature {
    fn from_v1(finality_signature: &FinalitySignatureV1) -> Self {
        LegacyFinalitySignature {
            block_hash: *finality_signature.block_hash(),
            era_id: finality_signature.era_id(),
            signature: *finality_signature.signature(),
            public_key: finality_signature.public_key().clone(),
        }
    }

    fn from_v2(finality_signature: &FinalitySignatureV2) -> Self {
        LegacyFinalitySignature {
            block_hash: *finality_signature.block_hash(),
            era_id: finality_signature.era_id(),
            signature: *finality_signature.signature(),
            public_key: finality_signature.public_key().clone(),
        }
    }
}

impl LegacySseData {
    pub fn from(data: &SseData) -> Option<Self> {
        match data {
            SseData::ApiVersion(protocol_version) => {
                Some(LegacySseData::ApiVersion(*protocol_version))
            }
            SseData::SidecarVersion(_) => None,
            SseData::Shutdown => None,
            SseData::BlockAdded { block_hash, block } => {
                build_default_block_added_translator().translate(block_hash, block)
            }
            SseData::TransactionAccepted(transaction) => {
                maybe_translate_transaction_accepted(transaction)
            }
            SseData::TransactionProcessed {
                transaction_hash,
                initiator_addr,
                timestamp,
                ttl,
                block_hash,
                execution_result,
                messages: _,
            } => maybe_translate_transaction_processed(
                transaction_hash,
                initiator_addr,
                timestamp,
                ttl,
                block_hash,
                execution_result,
            ),
            SseData::TransactionExpired { transaction_hash } => {
                maybe_translate_deploy_expired(transaction_hash)
            }
            SseData::Fault {
                era_id,
                public_key,
                timestamp,
            } => maybe_translate_fault(era_id, public_key, timestamp),
            SseData::FinalitySignature(fs) => Some(translate_finality_signature(fs)),
            SseData::Step { .. } => None, //we don't translate steps
        }
    }
}

fn translate_finality_signature(fs: &FinalitySignature) -> LegacySseData {
    match fs {
        FinalitySignature::V1(v1) => {
            LegacySseData::FinalitySignature(LegacyFinalitySignature::from_v1(v1))
        }
        FinalitySignature::V2(v2) => {
            LegacySseData::FinalitySignature(LegacyFinalitySignature::from_v2(v2))
        }
    }
}

fn maybe_translate_fault(
    era_id: &EraId,
    public_key: &PublicKey,
    timestamp: &Timestamp,
) -> Option<LegacySseData> {
    Some(LegacySseData::Fault {
        era_id: *era_id,
        public_key: public_key.clone(),
        timestamp: *timestamp,
    })
}

fn maybe_translate_deploy_expired(transaction_hash: &TransactionHash) -> Option<LegacySseData> {
    match transaction_hash {
        TransactionHash::Deploy(deploy_hash) => Some(LegacySseData::DeployExpired {
            deploy_hash: *deploy_hash,
        }),
        TransactionHash::V1(_) => None,
    }
}

fn maybe_translate_transaction_processed(
    transaction_hash: &TransactionHash,
    initiator_addr: &InitiatorAddr,
    timestamp: &Timestamp,
    ttl: &TimeDiff,
    block_hash: &BlockHash,
    execution_result: &ExecutionResult,
) -> Option<LegacySseData> {
    match transaction_hash {
        TransactionHash::Deploy(deploy_hash) => {
            let account = match initiator_addr {
                InitiatorAddr::PublicKey(public_key) => public_key,
                InitiatorAddr::AccountHash(_) => return None, //This shouldn't happen since we already are in TransactionHash::Deploy
            };
            let execution_result = match execution_result {
                ExecutionResult::V1(result) => result.clone(),
                ExecutionResult::V2(result) => {
                    let maybe_result =
                        build_default_execution_result_translator().translate(result);
                    maybe_result?
                }
            };
            Some(LegacySseData::DeployProcessed {
                deploy_hash: Box::new(*deploy_hash),
                account: Box::new(account.clone()),
                timestamp: *timestamp,
                ttl: *ttl,
                dependencies: Vec::new(),
                block_hash: Box::new(*block_hash),
                execution_result: Box::new(execution_result),
            })
        }
        _ => None, //V1 transactions can't be interpreted in the old format.
    }
}

fn maybe_translate_transaction_accepted(transaction: &Transaction) -> Option<LegacySseData> {
    match transaction {
        Transaction::Deploy(deploy) => Some(LegacySseData::DeployAccepted(deploy.clone())),
        _ => None, //V2 transactions can't be interpreted in the old format.
    }
}

#[cfg(test)]
mod tests {
    use super::fixtures::*;
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn should_translate_sse_to_legacy() {
        for (sse_data, expected, scenario_name) in sse_translation_scenarios() {
            let legacy_fs = LegacySseData::from(&sse_data);
            assert_eq!(
                legacy_fs,
                expected,
                "Failed when executing scenario {}",
                scenario_name.as_str()
            );
        }
    }

    #[allow(clippy::too_many_lines)]
    fn sse_translation_scenarios() -> Vec<(SseData, Option<LegacySseData>, String)> {
        vec![
            (
                api_version(),
                Some(legacy_api_version()),
                "api_version".to_string(),
            ),
            (
                finality_signature_v1(),
                Some(legacy_finality_signature()),
                "finality_signature_v1".to_string(),
            ),
            (
                finality_signature_v2(),
                Some(legacy_finality_signature()),
                "finality_signature_v2".to_string(),
            ),
            (
                transaction_accepted(),
                None,
                "transaction_accepted".to_string(),
            ),
            (
                deploy_accepted(),
                Some(legacy_deploy_accepted()),
                "legacy_deploy_accepted".to_string(),
            ),
            (
                deploy_expired(),
                Some(legacy_deploy_expired()),
                "legacy_deploy_expired".to_string(),
            ),
            (
                transaction_expired(),
                None,
                "transaction_expired".to_string(),
            ),
            (fault(), Some(legacy_fault()), "fault".to_string()),
            (
                block_added_v1(),
                Some(legacy_block_added()),
                "block_added_v1".to_string(),
            ),
            (
                block_added_v2(),
                Some(legacy_block_added_from_v2()),
                "block_added_v2".to_string(),
            ),
            (
                deploy_processed(),
                Some(legacy_deploy_processed()),
                "deploy_processed".to_string(),
            ),
        ]
    }
}
