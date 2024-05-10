use super::{
    structs,
    translate_deploy_hashes::{
        DeployHashTranslator, StandardDeployHashesTranslator, TransferDeployHashesTranslator,
    },
    LegacySseData,
};
use casper_types::{Block, BlockHash, BlockV2, EraEndV1, EraEndV2, EraReport, U512};
use mockall::automock;
use std::collections::BTreeMap;

#[automock]
pub trait BlockV2Translator {
    fn translate(&self, block_v2: &BlockV2) -> Option<structs::BlockV1>;
}

#[automock]
pub trait BlockAddedTranslator {
    fn translate(&self, block_hash: &BlockHash, block: &Block) -> Option<LegacySseData>;
}

#[automock]
pub trait EraEndV2Translator {
    fn translate(&self, era_end_v2: &EraEndV2) -> Option<EraEndV1>;
}

#[derive(Default)]
pub struct DefaultEraEndV2Translator;

impl EraEndV2Translator for DefaultEraEndV2Translator {
    fn translate(&self, era_end: &EraEndV2) -> Option<EraEndV1> {
        let mut rewards = BTreeMap::new();
        for (k, v) in era_end.rewards().iter() {
            let max_u64 = U512::from(u64::MAX);
            if v.gt(&max_u64) {
                //We're not able to cast the reward to u64, so we skip this era end.
                return None;
            }
            rewards.insert(k.clone(), v.as_u64());
        }
        let era_report = EraReport::new(
            era_end.equivocators().to_vec(),
            rewards,
            era_end.inactive_validators().to_vec(),
        );
        Some(EraEndV1::new(
            era_report,
            era_end.next_era_validator_weights().clone(),
        ))
    }
}

pub struct DefaultBlockV2Translator<ET, DT, TT>
where
    ET: EraEndV2Translator,
    DT: DeployHashTranslator,
    TT: DeployHashTranslator,
{
    era_end_translator: ET,
    deploy_hash_translator: DT,
    transfer_hash_translator: TT,
}

impl<ET, DT, TT> BlockV2Translator for DefaultBlockV2Translator<ET, DT, TT>
where
    ET: EraEndV2Translator,
    DT: DeployHashTranslator,
    TT: DeployHashTranslator,
{
    fn translate(&self, block_v2: &BlockV2) -> Option<structs::BlockV1> {
        let header = block_v2.header();
        let parent_hash = *header.parent_hash();
        let body_hash = *header.body_hash();
        let state_root_hash = *header.state_root_hash();
        let random_bit = block_v2.header().random_bit();
        let accumulated_seed = *header.accumulated_seed();
        let era_end = header
            .era_end()
            .and_then(|era_end| self.era_end_translator.translate(era_end));
        let timestamp = block_v2.header().timestamp();
        let era_id = block_v2.header().era_id();
        let height = block_v2.header().height();
        let protocol_version = block_v2.header().protocol_version();
        let block_hash = block_v2.hash();
        let body = block_v2.body();
        let proposer = header.proposer().clone();
        let deploy_hashes = self.deploy_hash_translator.translate(body);
        let transfer_hashes = self.transfer_hash_translator.translate(body);
        let block_v1 = structs::BlockV1::new(
            parent_hash,
            state_root_hash,
            body_hash,
            random_bit,
            accumulated_seed,
            era_end,
            timestamp,
            era_id,
            height,
            protocol_version,
            proposer,
            *block_hash,
            deploy_hashes,
            transfer_hashes,
        );
        Some(block_v1)
    }
}

pub struct DefaultBlockAddedTranslator<BT>
where
    BT: BlockV2Translator,
{
    block_v2_translator: BT,
}

pub fn build_default_block_added_translator() -> DefaultBlockAddedTranslator<
    DefaultBlockV2Translator<
        DefaultEraEndV2Translator,
        StandardDeployHashesTranslator,
        TransferDeployHashesTranslator,
    >,
> {
    DefaultBlockAddedTranslator {
        block_v2_translator: build_default_block_v2_translator(),
    }
}

pub fn build_default_block_v2_translator() -> DefaultBlockV2Translator<
    DefaultEraEndV2Translator,
    StandardDeployHashesTranslator,
    TransferDeployHashesTranslator,
> {
    DefaultBlockV2Translator {
        era_end_translator: DefaultEraEndV2Translator,
        deploy_hash_translator: StandardDeployHashesTranslator,
        transfer_hash_translator: TransferDeployHashesTranslator,
    }
}

impl<T> BlockAddedTranslator for DefaultBlockAddedTranslator<T>
where
    T: BlockV2Translator,
{
    fn translate(&self, block_hash: &BlockHash, block: &Block) -> Option<LegacySseData> {
        match block {
            Block::V1(block_v1) => Some(LegacySseData::BlockAdded {
                block_hash: *block_hash,
                block: structs::BlockV1::from(*block_v1.hash(), block_v1.header(), block_v1.body()),
            }),
            Block::V2(block_v2) => {
                let maybe_block = self.block_v2_translator.translate(block_v2);
                maybe_block.map(|block| LegacySseData::BlockAdded {
                    block_hash: *block_hash,
                    block,
                })
            }
        }
    }
}
