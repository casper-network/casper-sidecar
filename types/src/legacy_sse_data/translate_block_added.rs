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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use casper_types::{testing::TestRng, DeployHash, EraEndV1, EraReport, PublicKey, U512};
    use mockall::predicate;
    use pretty_assertions::assert_eq;
    use rand::Rng;
    use serde::Serialize;

    use super::{
        BlockV2Translator, DefaultBlockV2Translator, DefaultEraEndV2Translator, EraEndV2Translator,
        MockEraEndV2Translator,
    };
    use crate::{
        legacy_sse_data::{fixtures::*, translate_deploy_hashes::MockDeployHashTranslator},
        testing::parse_public_key,
    };

    #[test]
    pub fn default_block_v2_translator_translates_without_era_end_and_deploys() {
        let (mut era_end_translator, mut deploy_hash_translator, mut transfer_hash_translator) =
            prepare_mocks();
        let block_v2 = block_v2();
        let era_end_ref = block_v2.header().era_end().unwrap();
        prepare_era_end_mock(&mut era_end_translator, era_end_ref, None);
        prepare_deploys_mock(&mut deploy_hash_translator, &block_v2, vec![]);
        prepare_transfer_mock(&mut transfer_hash_translator, &block_v2, vec![]);
        let under_test = DefaultBlockV2Translator {
            era_end_translator,
            deploy_hash_translator,
            transfer_hash_translator,
        };

        let got = under_test.translate(&block_v2);
        assert!(got.is_some());
        let expected = block_v1_no_deploys_no_era();
        compare_as_json(&expected, &got.unwrap());
    }

    #[test]
    pub fn default_block_v2_translator_passes_era_end_info_and_deploys() {
        let mut test_rng = TestRng::new();
        let (mut era_end_translator, mut deploy_hash_translator, mut transfer_hash_translator) =
            prepare_mocks();
        let block_v2 = block_v2();
        let era_end_ref = block_v2.header().era_end().unwrap();
        let report = EraReport::random(&mut test_rng);
        let validator_weights = random_validator_weights(&mut test_rng);
        let era_end = EraEndV1::new(report, validator_weights);
        let deploy_hashes_1: Vec<DeployHash> =
            (0..3).map(|_| DeployHash::random(&mut test_rng)).collect();
        let deploy_hashes_2: Vec<DeployHash> =
            (0..3).map(|_| DeployHash::random(&mut test_rng)).collect();
        prepare_era_end_mock(&mut era_end_translator, era_end_ref, Some(era_end.clone()));
        prepare_deploys_mock(
            &mut deploy_hash_translator,
            &block_v2,
            deploy_hashes_1.clone(),
        );
        prepare_transfer_mock(
            &mut transfer_hash_translator,
            &block_v2,
            deploy_hashes_2.clone(),
        );

        let under_test = DefaultBlockV2Translator {
            era_end_translator,
            deploy_hash_translator,
            transfer_hash_translator,
        };

        let got = under_test.translate(&block_v2).unwrap();
        assert_eq!(got.body.deploy_hashes, deploy_hashes_1);
        assert_eq!(got.body.transfer_hashes, deploy_hashes_2);
    }

    #[test]
    fn default_era_end_v2_translator_translates_all_data() {
        let under_test = DefaultEraEndV2Translator;
        let era_end_v2 = era_end_v2();
        let maybe_translated = under_test.translate(&era_end_v2);
        assert!(maybe_translated.is_some());
        let translated = maybe_translated.unwrap();
        let mut expected_validator_weights = BTreeMap::new();
        expected_validator_weights.insert(
            parse_public_key("013183e7169846881fb6ae07dc5ba63d92bd592d67681a765cf9813d5146de97f3"),
            U512::from(277433153),
        );
        expected_validator_weights.insert(
            parse_public_key(
                "0203ff1caebb0fd53fb4c52f8cf18d0e3257f6b075a16f0fd6aa5499ddb28a8d82ab",
            ),
            U512::from(818689728),
        );
        let mut rewards = BTreeMap::new();
        rewards.insert(
            parse_public_key("010d2d4fdda5ff7a9820de2fe18a262b29bb2df36cbc767446e1dcd015e9c5ea98"),
            155246594,
        );
        let report = EraReport::new(
            vec![
                parse_public_key(
                    "010a10a45ea0aff7af1ffef92287d00ec4cf01c5e9e2952e018a2fbb0f0ede2b50",
                ),
                parse_public_key(
                    "02037c17d279d6e54375f7cfb3559730d5434bfedc8638a3f95e55f6e85fc9e8f611",
                ),
                parse_public_key(
                    "02026d4b741a0ece4b3d6d61294a8db28a28dbd734133694582d38f240686ec61d05",
                ),
            ],
            rewards,
            vec![parse_public_key(
                "010a10a45ea0aff7af1ffef92287d00ec4cf01c5e9e2952e018a2fbb0f0ede2b51",
            )],
        );
        let expected = EraEndV1::new(report, expected_validator_weights);
        assert_eq!(translated, expected);
    }

    #[test]
    fn default_era_end_v2_translator_returns_none_when_reward_exceeds_u64() {
        let under_test = DefaultEraEndV2Translator;
        let era_end_v2 = era_end_v2_with_reward_exceeding_u64();
        let maybe_translated = under_test.translate(&era_end_v2);
        assert!(maybe_translated.is_none());
    }

    fn compare_as_json<T, Y>(left: &T, right: &Y)
    where
        T: Serialize,
        Y: Serialize,
    {
        let left_value = serde_json::to_value(left).unwrap();
        let right_value = serde_json::to_value(right).unwrap();
        assert_eq!(left_value, right_value);
    }

    fn prepare_deploys_mock(
        deploy_hash_translator: &mut MockDeployHashTranslator,
        block_v2: &casper_types::BlockV2,
        deploys: Vec<DeployHash>,
    ) {
        deploy_hash_translator
            .expect_translate()
            .times(1)
            .with(predicate::eq(block_v2.body().clone()))
            .return_const(deploys);
    }

    fn prepare_transfer_mock(
        transfer_hash_translator: &mut MockDeployHashTranslator,
        block_v2: &casper_types::BlockV2,
        deploys: Vec<DeployHash>,
    ) {
        transfer_hash_translator
            .expect_translate()
            .times(1)
            .with(predicate::eq(block_v2.body().clone()))
            .return_const(deploys);
    }

    fn prepare_era_end_mock(
        era_end_translator: &mut MockEraEndV2Translator,
        era_end_ref: &casper_types::EraEndV2,
        returned: Option<EraEndV1>,
    ) {
        era_end_translator
            .expect_translate()
            .times(1)
            .with(predicate::eq(era_end_ref.clone()))
            .return_const(returned);
    }

    fn prepare_mocks() -> (
        MockEraEndV2Translator,
        MockDeployHashTranslator,
        MockDeployHashTranslator,
    ) {
        let era_end_translator = MockEraEndV2Translator::new();
        let deploy_hash_translator = MockDeployHashTranslator::new();
        let transfer_hash_translator = MockDeployHashTranslator::new();
        (
            era_end_translator,
            deploy_hash_translator,
            transfer_hash_translator,
        )
    }

    fn random_validator_weights(
        test_rng: &mut TestRng,
    ) -> std::collections::BTreeMap<casper_types::PublicKey, casper_types::U512> {
        let mut tree = BTreeMap::new();
        let number_of_weights = test_rng.gen_range(5..=10);
        for _ in 0..number_of_weights {
            tree.insert(PublicKey::random(test_rng), test_rng.gen());
        }
        tree
    }
}
