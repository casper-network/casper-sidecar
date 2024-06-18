use std::collections::{BTreeMap, BTreeSet};
use std::str::FromStr;

use casper_types::system::auction::ValidatorWeights;
use casper_types::{
    BlockHash, BlockV2, Deploy, DeployHash, Digest, EraEndV2, EraId, ProtocolVersion, PublicKey,
    RewardedSignatures, SingleBlockRewardedSignatures, TimeDiff, Timestamp, Transaction,
    TransactionV1, TransactionV1Hash, U512,
};
use rand::Rng;

use super::{structs, LegacySseData};
use crate::sse_data::SseData;
use crate::testing::{parse_block_hash, parse_digest, parse_public_key};
use casper_types::testing::TestRng;
use casper_types::TestBlockBuilder;

pub fn legacy_block_added() -> LegacySseData {
    serde_json::from_str(RAW_LEGACY_BLOCK_ADDED).unwrap()
}

pub fn legacy_block_added_from_v2() -> LegacySseData {
    serde_json::from_str(RAW_LEGACY_BLOCK_ADDED_FROM_V2).unwrap()
}

pub fn block_added_v1() -> SseData {
    serde_json::from_str(RAW_BLOCK_ADDED_V1).unwrap()
}

pub fn block_added_v2() -> SseData {
    serde_json::from_str(RAW_BLOCK_ADDED_V2).unwrap()
}

pub fn api_version() -> SseData {
    serde_json::from_str(RAW_API_VERSION).unwrap()
}

pub fn legacy_api_version() -> LegacySseData {
    serde_json::from_str(RAW_API_VERSION).unwrap()
}

pub fn finality_signature_v1() -> SseData {
    serde_json::from_str(RAW_FINALITY_SIGNATURE_V1).unwrap()
}

pub fn finality_signature_v2() -> SseData {
    serde_json::from_str(RAW_FINALITY_SIGNATURE_V2).unwrap()
}

pub fn transaction_accepted() -> SseData {
    serde_json::from_str(RAW_TRANSACTION_ACCEPTED).unwrap()
}

pub fn deploy_accepted() -> SseData {
    serde_json::from_str(RAW_DEPLOY_ACCEPTED).unwrap()
}

pub fn legacy_deploy_accepted() -> LegacySseData {
    serde_json::from_str(RAW_LEGACY_DEPLOY_ACCEPTED).unwrap()
}

pub fn deploy_expired() -> SseData {
    serde_json::from_str(RAW_DEPLOY_EXPIRED).unwrap()
}

pub fn transaction_expired() -> SseData {
    serde_json::from_str(RAW_TRANSACTION_EXPIRED).unwrap()
}

pub fn legacy_deploy_expired() -> LegacySseData {
    serde_json::from_str(RAW_LEGACY_DEPLOY_EXPIRED).unwrap()
}

pub fn legacy_finality_signature() -> LegacySseData {
    serde_json::from_str(RAW_LEGACY_FINALITY_SIGNATURE).unwrap()
}

pub fn fault() -> SseData {
    serde_json::from_str(RAW_FAULT).unwrap()
}

pub fn legacy_fault() -> LegacySseData {
    serde_json::from_str(RAW_LEGACY_FAULT).unwrap()
}

pub fn deploy_processed() -> SseData {
    serde_json::from_str(RAW_DEPLOY_PROCESSED).unwrap()
}

pub fn legacy_deploy_processed() -> LegacySseData {
    serde_json::from_str(RAW_LEGACY_DEPLOY_PROCESSED).unwrap()
}

pub fn parent_hash() -> BlockHash {
    parse_block_hash("90a4ade2849634e9c1ad0e02cb30645d0984056f68075cad8f6cad2b42a824ba")
}

pub fn state_root_hash() -> Digest {
    parse_digest("9cce223fdbeab41dbbcf0b62f3fd857373131378d51776de26bb9f4fefe1e849")
}

pub fn timestamp() -> Timestamp {
    Timestamp::from_str("2020-08-07T01:30:25.521Z").unwrap()
}

pub fn proposer() -> PublicKey {
    parse_public_key("0203426736da2554ebf1f8ee1d2ce4ab11b1e33419d7dfc1ce2fe1945faf00bacc9e")
}

#[allow(clippy::too_many_arguments)]
pub fn block_v2_with_transactions(
    rng: &mut TestRng,
    parent_hash: BlockHash,
    state_root_hash: Digest,
    timestamp: Timestamp,
    era_id: EraId,
    height: u64,
    proposer: PublicKey,
    transactions: Vec<&Transaction>,
) -> BlockV2 {
    let mut validator_weights = ValidatorWeights::new();
    let key_1 =
        parse_public_key("0198957673ad060503e2ec7d98dc71af6f90ad1f854fe18025e3e7d0d1bbe5e32b");
    let key_2 =
        parse_public_key("02022d6bc4e3012cc4ae467b5525111cf7ed65883b05a1d924f1e654c64fad3a027c");
    let key_3 =
        parse_public_key("0202fd52dbda97f41def3e3252704d5f8f5adbec1919368282e02e9500bd88845a80");
    let key_4 =
        parse_public_key("02022d6bc4e3012cc4ae467b5525111cf7ed65883b05a1d924f1e654c64fad3a027e");
    validator_weights.insert(key_1.clone(), U512::from_dec_str("1").unwrap());
    validator_weights.insert(key_2.clone(), U512::from_dec_str("2").unwrap());
    let mut public_keys = BTreeSet::new();
    public_keys.insert(key_1.clone());
    public_keys.insert(key_4.clone());
    let all_validators = vec![&key_1, &key_2, &key_3, &key_4];
    let single_block_sigs =
        SingleBlockRewardedSignatures::from_validator_set(&public_keys, all_validators);
    let rewarded_signatures = RewardedSignatures::new(vec![single_block_sigs]);
    TestBlockBuilder::default()
        .parent_hash(parent_hash)
        .state_root_hash(state_root_hash)
        .timestamp(timestamp)
        .era(era_id)
        .height(height)
        .protocol_version(ProtocolVersion::V2_0_0)
        .proposer(proposer)
        .switch_block(true)
        .validator_weights(validator_weights)
        .rewarded_signatures(rewarded_signatures)
        .transactions(transactions)
        .build(rng)
}

pub fn sample_transactions(
    rng: &mut TestRng,
) -> (
    Vec<Transaction>,
    DeployHash,
    TransactionV1Hash,
    DeployHash,
    TransactionV1Hash,
    TransactionV1Hash,
    TransactionV1Hash,
) {
    let timestamp = Timestamp::now();
    let ttl = TimeDiff::from_seconds(rng.gen_range(60..300));

    let deploy = Deploy::random_with_valid_session_package_by_name(rng);
    let standard_deploy_hash = *deploy.hash();
    let standard_deploy = Transaction::Deploy(deploy);

    let version_1 = TransactionV1::random(rng);
    let standard_version_1_hash = *version_1.hash();
    let standard_version_1 = Transaction::V1(version_1);

    let deploy = Deploy::random_valid_native_transfer_with_timestamp_and_ttl(rng, timestamp, ttl);
    let mint_deploy_hash = *deploy.hash();
    let mint_deploy = Transaction::Deploy(deploy);

    let version_1 = TransactionV1::random_transfer(rng, Some(timestamp), Some(ttl));
    let mint_version_1_hash = *version_1.hash();
    let mint_version_1 = Transaction::V1(version_1);

    let version_1 = TransactionV1::random_install_upgrade(rng, Some(timestamp), Some(ttl));
    let install_upgrade_v1_hash = *version_1.hash();
    let install_upgrade_v1 = Transaction::V1(version_1);

    let version_1 = TransactionV1::random_wasm(rng, Some(timestamp), Some(ttl));
    let auction_v1_hash = *version_1.hash();
    let auction_v1 = Transaction::V1(version_1);

    (
        vec![
            standard_deploy,
            standard_version_1,
            mint_deploy,
            mint_version_1,
            install_upgrade_v1,
            auction_v1,
        ],
        standard_deploy_hash,
        standard_version_1_hash,
        mint_deploy_hash,
        mint_version_1_hash,
        install_upgrade_v1_hash,
        auction_v1_hash,
    )
}

pub fn block_v2(
    rng: &mut TestRng,
    parent_hash: BlockHash,
    state_root_hash: Digest,
    timestamp: Timestamp,
    era_id: EraId,
    height: u64,
    proposer: PublicKey,
) -> BlockV2 {
    block_v2_with_transactions(
        rng,
        parent_hash,
        state_root_hash,
        timestamp,
        era_id,
        height,
        proposer,
        vec![],
    )
}

#[allow(clippy::too_many_arguments)]
pub fn block_v1_no_deploys_no_era(
    parent_hash: BlockHash,
    state_root_hash: Digest,
    body_hash: Digest,
    random_bit: bool,
    accumulated_seed: Digest,
    timestamp: Timestamp,
    era_id: EraId,
    height: u64,
    proposer: PublicKey,
    block_hash: BlockHash,
) -> structs::BlockV1 {
    structs::BlockV1::new(
        parent_hash,
        state_root_hash,
        body_hash,
        random_bit,
        accumulated_seed,
        None,
        timestamp,
        era_id,
        height,
        ProtocolVersion::V2_0_0,
        proposer,
        block_hash,
        vec![],
        vec![],
    )
}

pub fn era_end_v2() -> EraEndV2 {
    let mut next_era_validator_weights = BTreeMap::new();
    next_era_validator_weights.insert(
        parse_public_key("0198957673ad060503e2ec7d98dc71af6f90ad1f854fe18025e3e7d0d1bbe5e32b"),
        U512::from_dec_str("1").unwrap(),
    );
    next_era_validator_weights.insert(
        parse_public_key("02022d6bc4e3012cc4ae467b5525111cf7ed65883b05a1d924f1e654c64fad3a027c"),
        U512::from_dec_str("2").unwrap(),
    );
    let mut rewards = BTreeMap::new();
    rewards.insert(
        parse_public_key("01235b932586ae5cc3135f7a0dc723185b87e5bd3ae0ac126a92c14468e976ff25"),
        vec![U512::from_dec_str("129457537").unwrap()],
    );
    EraEndV2::new(
        vec![
            parse_public_key("010a10a45ea0aff7af1ffef92287d00ec4cf01c5e9e2952e018a2fbb0f0ede2b50"),
            parse_public_key(
                "02037c17d279d6e54375f7cfb3559730d5434bfedc8638a3f95e55f6e85fc9e8f611",
            ),
            parse_public_key(
                "02026d4b741a0ece4b3d6d61294a8db28a28dbd734133694582d38f240686ec61d05",
            ),
        ],
        vec![parse_public_key(
            "010a10a45ea0aff7af1ffef92287d00ec4cf01c5e9e2952e018a2fbb0f0ede2b51",
        )],
        next_era_validator_weights,
        rewards,
        1,
    )
}

pub fn era_end_v2_with_reward_exceeding_u64() -> EraEndV2 {
    let mut next_era_validator_weights = BTreeMap::new();
    next_era_validator_weights.insert(
        parse_public_key("0198957673ad060503e2ec7d98dc71af6f90ad1f854fe18025e3e7d0d1bbe5e32b"),
        U512::from_dec_str("1").unwrap(),
    );
    next_era_validator_weights.insert(
        parse_public_key("02022d6bc4e3012cc4ae467b5525111cf7ed65883b05a1d924f1e654c64fad3a027c"),
        U512::from_dec_str("2").unwrap(),
    );
    let mut rewards = BTreeMap::new();
    rewards.insert(
        parse_public_key("01235b932586ae5cc3135f7a0dc723185b87e5bd3ae0ac126a92c14468e976ff25"),
        vec![U512::from_dec_str("18446744073709551616").unwrap()],
    );
    EraEndV2::new(
        vec![
            parse_public_key("010a10a45ea0aff7af1ffef92287d00ec4cf01c5e9e2952e018a2fbb0f0ede2b50"),
            parse_public_key(
                "02037c17d279d6e54375f7cfb3559730d5434bfedc8638a3f95e55f6e85fc9e8f611",
            ),
            parse_public_key(
                "02026d4b741a0ece4b3d6d61294a8db28a28dbd734133694582d38f240686ec61d05",
            ),
        ],
        vec![parse_public_key(
            "010a10a45ea0aff7af1ffef92287d00ec4cf01c5e9e2952e018a2fbb0f0ede2b51",
        )],
        next_era_validator_weights,
        rewards,
        1,
    )
}

const RAW_API_VERSION: &str = r#"{"ApiVersion":"2.0.0"}"#;

const RAW_FINALITY_SIGNATURE_V2: &str = r#"{
    "FinalitySignature": {
      "V2": {
        "block_hash": "45d7c385cba0a880cbc0068ccc6c58111d057d8190850b744c8e0450a24639d4",
        "block_height": 60923,
        "era_id": 139,
        "chain_name_hash": "f087a92e6e7077b3deb5e00b14a904e34c7068a9410365435bc7ca5d3ac64301",
        "signature": "01cc39996b8410b500a61c97f888b381546de77e13e8af1a509d3305021b079c1d54b29f3eac8370eb40af2a6419b81427e09cd3c2e72567357fa2120abb0bba06",
        "public_key": "017536433a73f7562526f3e9fcb8d720428ae2d28788a9909f3c6f637a9d848a4b"
      }
    }
  }"#;

const RAW_FINALITY_SIGNATURE_V1: &str = r#"{
"FinalitySignature": {
    "V1": {
      "block_hash": "45d7c385cba0a880cbc0068ccc6c58111d057d8190850b744c8e0450a24639d4",
      "era_id": 139,
      "signature": "01cc39996b8410b500a61c97f888b381546de77e13e8af1a509d3305021b079c1d54b29f3eac8370eb40af2a6419b81427e09cd3c2e72567357fa2120abb0bba06",
      "public_key": "017536433a73f7562526f3e9fcb8d720428ae2d28788a9909f3c6f637a9d848a4b"
    }
}
}"#;

const RAW_LEGACY_FINALITY_SIGNATURE: &str = r#"{
    "FinalitySignature": {
        "block_hash": "45d7c385cba0a880cbc0068ccc6c58111d057d8190850b744c8e0450a24639d4",
        "era_id": 139,
        "signature": "01cc39996b8410b500a61c97f888b381546de77e13e8af1a509d3305021b079c1d54b29f3eac8370eb40af2a6419b81427e09cd3c2e72567357fa2120abb0bba06",
        "public_key": "017536433a73f7562526f3e9fcb8d720428ae2d28788a9909f3c6f637a9d848a4b"
    }
    }"#;

const RAW_TRANSACTION_ACCEPTED: &str = r#"
{
    "TransactionAccepted": {
        "Version1": {
            "hash": "2084a40f58874fb2997e029e61ec55e3d5a6cd5f6de77a1d42dcaf21aeddc760",
            "header": {
                "chain_name":"⸻⋉◬⸗ⶨ⼄≙⡫⨁ⶃℍ⊨⇏ⴲⲋ⪝⣬ⴂ⨨⪯⿉⺙⚚⻰⒯ⶖ⟽⬪❴⴯╽♥⅏⏵❲⃽ⶁ⾠⸗◩⋑Ⅹ♼⺓⊻⼠Ⓩ∇Ⅺ⸔◘⠝◓⚾◯⦁★⢹␄⍆⨿⵮⭭⮛⸹⃻⹶⎶⟆⛎⤑₇⩐╨⋸⠸₈⥡ⷔ⹪⤛⭺⵫Ⲗ⃁⪏⫵⚎⁘⦳☉␛Ⲹ⥝⇡Ⰰ⫂⁎⍆⼸",
                "timestamp": "2020-08-07T01:30:25.521Z",
                "ttl": "5h 6m 46s 219ms",
                "body_hash": "11ddedb85acbe04217e4f322663e7a3b90630321cdff7d7a8f0ce97fd76ead9a",
                "pricing_mode": {
                    "Fixed": {
                        "gas_price_tolerance": 5
                    }
                },
                "initiator_addr": {
                    "PublicKey": "01b0c1bc1910f3e2e5fa8329d642b34e72e34183e0a2b239021906df8d7d968fcd"
                }
            },
            "body": {
                "args": [
                    [
                        "source",
                        {
                            "cl_type": {
                                "Option": "URef"
                            },
                            "bytes": "01d4ce239a968d7ac214964f714f6aa267612d1da1ec9c65dfc40a99d0e1a673ce02",
                            "parsed": "uref-d4ce239a968d7ac214964f714f6aa267612d1da1ec9c65dfc40a99d0e1a673ce-002"
                        }
                    ],
                    [
                        "target",
                        {
                            "cl_type": "PublicKey",
                            "bytes": "015a977c34eeff036613837814822a1a44986f2a7057c17436d01d200132614c58",
                            "parsed": "015a977c34eeff036613837814822a1a44986f2a7057c17436d01d200132614c58"
                        }
                    ],
                    [
                        "amount",
                        {
                            "cl_type": "U512",
                            "bytes": "08b30d8646748b0f87",
                            "parsed": "9732150651286588851"
                        }
                    ],
                    [
                        "id",
                        {
                            "cl_type": {
                                "Option": "U64"
                            },
                            "bytes": "01dfd56bb1e2ac2494",
                            "parsed": 10674847106414138847
                        }
                    ]
                ],
                "target": "Native",
                "entry_point": "Transfer",
                "scheduling": {
                    "FutureTimestamp": "2020-08-07T01:32:59.428Z"
                },
                "transaction_category": 0
            },
            "approvals": [
                {
                    "signer": "01b0c1bc1910f3e2e5fa8329d642b34e72e34183e0a2b239021906df8d7d968fcd",
                    "signature": "01fb52d40bd36c813ca69b982f6b7f4bac79314187e51e69128fa4d87fbb2cfe8e803b2eedaa6f39566ca3a4dc59ac418824aa2e7fc05611910162cf9f6a164902"
                }
            ]
        }
    }
}
"#;

const RAW_LEGACY_DEPLOY_ACCEPTED: &str = r#"
{
    "DeployAccepted": {
        "hash": "5a7709969c210db93d3c21bf49f8bf705d7c75a01609f606d04b0211af171d43",
        "header": {
            "account": "02022c07e061d6e0b43bbaa25717b021c2ddc0f701a223946a0883b57ae842917438",
            "timestamp": "2020-08-07T01:28:27.360Z",
            "ttl": "4m 22s",
            "gas_price": 72,
            "body_hash": "aa2a111c086628a161001160756c5884e32fde0356bb85f484a3e55682ad089f",
            "dependencies": [],
            "chain_name": "casper-example"
        },
        "payment": {
            "StoredContractByName": {
                "name": "casper-example",
                "entry_point": "example-entry-point",
                "args": [
                    [
                        "amount",
                        {
                            "cl_type": "U512",
                            "bytes": "0400f90295",
                            "parsed": "2500000000"
                        }
                    ]
                ]
            }
        },
        "session": {
            "StoredContractByHash": {
                "hash": "dfb621e7012df48fe1d40fd8015b5e2396c477c9587e996678551148a06d3a89",
                "entry_point": "8sY9fUUCwoiFZmxKo8kj",
                "args": [
                    [
                        "YbZWtEuL4D6oMTJmUWvj",
                        {
                            "cl_type": {
                                "List": "U8"
                            },
                            "bytes": "5a000000909ffe7807b03a5db0c3c183648710db16d408d8425a4e373fc0422a4efed1ab0040bc08786553fcac4521528c9fafca0b0fb86f4c6e9fb9db7a1454dda8ed612c4ea4c9a6378b230ae1e3c236e37d6ebee94339a56cb4be582a",
                            "parsed": [
                                144,
                                159,
                                254,
                                120,
                                7
                            ]
                        }
                    ]
                ]
            }
        },
        "approvals": [
            {
                "signer": "02022c07e061d6e0b43bbaa25717b021c2ddc0f701a223946a0883b57ae842917438",
                "signature": "025d0a7ba37bebe6774681ca5adecb70fa4eef56821eb344bf0f6867e171a899a87edb2b8bf70f2cb47a1670a6baf2cded1fad535ee53a2f65da91c82ebf30945b"
            }
        ]
    }
}"#;

const RAW_DEPLOY_ACCEPTED: &str = r#"
{
    "TransactionAccepted": {
        "Deploy": {
            "hash": "5a7709969c210db93d3c21bf49f8bf705d7c75a01609f606d04b0211af171d43",
            "header": {
                "account": "02022c07e061d6e0b43bbaa25717b021c2ddc0f701a223946a0883b57ae842917438",
                "timestamp": "2020-08-07T01:28:27.360Z",
                "ttl": "4m 22s",
                "gas_price": 72,
                "body_hash": "aa2a111c086628a161001160756c5884e32fde0356bb85f484a3e55682ad089f",
                "dependencies": [],
                "chain_name": "casper-example"
            },
            "payment": {
                "StoredContractByName": {
                    "name": "casper-example",
                    "entry_point": "example-entry-point",
                    "args": [
                        [
                            "amount",
                            {
                                "cl_type": "U512",
                                "bytes": "0400f90295",
                                "parsed": "2500000000"
                            }
                        ]
                    ]
                }
            },
            "session": {
                "StoredContractByHash": {
                    "hash": "dfb621e7012df48fe1d40fd8015b5e2396c477c9587e996678551148a06d3a89",
                    "entry_point": "8sY9fUUCwoiFZmxKo8kj",
                    "args": [
                        [
                            "YbZWtEuL4D6oMTJmUWvj",
                            {
                                "cl_type": {
                                    "List": "U8"
                                },
                                "bytes": "5a000000909ffe7807b03a5db0c3c183648710db16d408d8425a4e373fc0422a4efed1ab0040bc08786553fcac4521528c9fafca0b0fb86f4c6e9fb9db7a1454dda8ed612c4ea4c9a6378b230ae1e3c236e37d6ebee94339a56cb4be582a",
                                "parsed": [
                                    144,
                                    159,
                                    254,
                                    120,
                                    7
                                ]
                            }
                        ]
                    ]
                }
            },
            "approvals": [
                {
                    "signer": "02022c07e061d6e0b43bbaa25717b021c2ddc0f701a223946a0883b57ae842917438",
                    "signature": "025d0a7ba37bebe6774681ca5adecb70fa4eef56821eb344bf0f6867e171a899a87edb2b8bf70f2cb47a1670a6baf2cded1fad535ee53a2f65da91c82ebf30945b"
                }
            ]
        }
    }
}
"#;

const RAW_DEPLOY_EXPIRED: &str = r#"
{
    "TransactionExpired": {
        "transaction_hash": {
            "Deploy": "565d7147e28be402c34208a133fd59fde7ac785ae5f0298cb5fb7adfb1b054a8"
        }
    }
}
"#;

const RAW_TRANSACTION_EXPIRED: &str = r#"
{
    "TransactionExpired": {
        "transaction_hash": {
            "Version1": "8c22ae866be3287b6374592083b17cbaf4b0452d7a55adb2a4e53bb0295c0d76"
        }
    }
}
"#;

const RAW_LEGACY_DEPLOY_EXPIRED: &str = r#"
{
    "DeployExpired": {
        "deploy_hash": "565d7147e28be402c34208a133fd59fde7ac785ae5f0298cb5fb7adfb1b054a8"
    }
}
"#;

const RAW_FAULT: &str = r#"
{
    "Fault": {
        "era_id": 769794,
        "public_key": "02034ce1acbceeb5eb2b20eeeef9965e3ca8e7a95655f2089342bcbb51319a0d70d1",
        "timestamp": "2020-08-07T01:30:59.692Z"
    }
}
"#;

const RAW_LEGACY_FAULT: &str = r#"
{
    "Fault": {
        "era_id": 769794,
        "public_key": "02034ce1acbceeb5eb2b20eeeef9965e3ca8e7a95655f2089342bcbb51319a0d70d1",
        "timestamp": "2020-08-07T01:30:59.692Z"
    }
}
"#;

const RAW_LEGACY_BLOCK_ADDED: &str = r#"
{
    "BlockAdded": {
        "block_hash": "d59359690ca5a251b513185da0767f744e77645adec82bb6ff785a89edc7591c",
        "block": {
            "hash": "d59359690ca5a251b513185da0767f744e77645adec82bb6ff785a89edc7591c",
            "header": {
                "parent_hash": "90ca56a697f8b1b19cba08c642fd7f04669b8cd49bb9d652fca989f8a9f8bcea",
                "state_root_hash": "9cce223fdbeab41dbbcf0b62f3fd857373131378d51776de26bb9f4fefe1e849",
                "body_hash": "5f37be399c15b2394af48243ce10a62a7d12769dc5f7740b18ad3bf55bde5271",
                "random_bit": true,
                "accumulated_seed": "b3e1930565a80a874a443eaadefa1a340927fb8b347729bbd93e93935a47a9e4",
                "era_end": {
                    "era_report": {
                        "equivocators": [
                            "0203c9da857cfeccf001ce00720ae2e0d083629858b60ac05dd285ce0edae55f0c8e",
                            "02026fb7b629a2ec0132505cdf036f6ffb946d03a1c9b5da57245af522b842f145be"
                        ],
                        "rewards": [
                            {
                                "validator": "01235b932586ae5cc3135f7a0dc723185b87e5bd3ae0ac126a92c14468e976ff25",
                                "amount": 129457537
                            }
                        ],
                        "inactive_validators": []
                    },
                    "next_era_validator_weights": [
                        {
                            "validator": "0198957673ad060503e2ec7d98dc71af6f90ad1f854fe18025e3e7d0d1bbe5e32b",
                            "weight": "1"
                        },
                        {
                            "validator": "02022d6bc4e3012cc4ae467b5525111cf7ed65883b05a1d924f1e654c64fad3a027c",
                            "weight": "2"
                        }
                    ]
                },
                "timestamp": "2024-04-25T20:00:35.640Z",
                "era_id": 601701,
                "height": 6017012,
                "protocol_version": "1.0.0"
            },
            "body": {
                "proposer": "0203426736da2554ebf1f8ee1d2ce4ab11b1e33419d7dfc1ce2fe1945faf00bacc9e",
                "deploy_hashes": [
                    "06950e4374dc88685634ec30bcddd68e6b46c109ccf6d29e2dfcf5367df75571",
                    "27a89dd58e6297a5244342b68b117afe2555131b896ad6ed4321edcd4130ae7b"
                ],
                "transfer_hashes": [
                    "3e30b6c1c5dbca9277425846b42dc832cd3d8ce889c38d6bfc8bd95b3e1c403e",
                    "c990ba47146270655eaacc53d4115cbd980697f3d4e9c76bccfdfce82af6ce08"
                ]
            }
        }
    }
}"#;

const RAW_BLOCK_ADDED_V1: &str = r#"
{
    "BlockAdded": {
        "block_hash": "d59359690ca5a251b513185da0767f744e77645adec82bb6ff785a89edc7591c",
        "block": {
            "Version1": {
                "hash": "d59359690ca5a251b513185da0767f744e77645adec82bb6ff785a89edc7591c",
                "header": {
                    "parent_hash": "90ca56a697f8b1b19cba08c642fd7f04669b8cd49bb9d652fca989f8a9f8bcea",
                    "state_root_hash": "9cce223fdbeab41dbbcf0b62f3fd857373131378d51776de26bb9f4fefe1e849",
                    "body_hash": "5f37be399c15b2394af48243ce10a62a7d12769dc5f7740b18ad3bf55bde5271",
                    "random_bit": true,
                    "accumulated_seed": "b3e1930565a80a874a443eaadefa1a340927fb8b347729bbd93e93935a47a9e4",
                    "era_end": {
                        "era_report": {
                            "equivocators": [
                                "0203c9da857cfeccf001ce00720ae2e0d083629858b60ac05dd285ce0edae55f0c8e",
                                "02026fb7b629a2ec0132505cdf036f6ffb946d03a1c9b5da57245af522b842f145be"
                            ],
                            "rewards": [
                                {
                                    "validator": "01235b932586ae5cc3135f7a0dc723185b87e5bd3ae0ac126a92c14468e976ff25",
                                    "amount": 129457537
                                }
                            ],
                            "inactive_validators": []
                        },
                        "next_era_validator_weights": [
                            {
                                "validator": "0198957673ad060503e2ec7d98dc71af6f90ad1f854fe18025e3e7d0d1bbe5e32b",
                                "weight": "1"
                            },
                            {
                                "validator": "02022d6bc4e3012cc4ae467b5525111cf7ed65883b05a1d924f1e654c64fad3a027c",
                                "weight": "2"
                            }
                        ]
                    },
                    "timestamp": "2024-04-25T20:00:35.640Z",
                    "era_id": 601701,
                    "height": 6017012,
                    "protocol_version": "1.0.0"
                },
                "body": {
                    "proposer": "0203426736da2554ebf1f8ee1d2ce4ab11b1e33419d7dfc1ce2fe1945faf00bacc9e",
                    "deploy_hashes": [
                    "06950e4374dc88685634ec30bcddd68e6b46c109ccf6d29e2dfcf5367df75571",
                    "27a89dd58e6297a5244342b68b117afe2555131b896ad6ed4321edcd4130ae7b"
                ],
                "transfer_hashes": [
                    "3e30b6c1c5dbca9277425846b42dc832cd3d8ce889c38d6bfc8bd95b3e1c403e",
                    "c990ba47146270655eaacc53d4115cbd980697f3d4e9c76bccfdfce82af6ce08"
                ]
                }
            }
        }
    }
}
"#;

const RAW_BLOCK_ADDED_V2: &str = r#"{
    "BlockAdded": {
        "block_hash": "2df9fb8909443fba928ed0536a79780cdb4557d0c05fdf762a1fd61141121422",
        "block": {
            "Version2": {
                "hash": "2df9fb8909443fba928ed0536a79780cdb4557d0c05fdf762a1fd61141121422",
                "header": {
                    "proposer": "01d3eec0445635f136ae560b43e9d8f656a6ba925f01293eaf2610b39ebe0fc28d",
                    "parent_hash": "b8f5e9afd2e54856aa1656f962d07158f0fdf9cfac0f9992875f31f6bf2623a2",
                    "state_root_hash": "cbf02d08bb263aa8915507c172b5f590bbddcd68693fb1c71758b5684b011730",
                    "body_hash": "6041ab862a1e14a43a8e8a9a42dad27091915a337d18060c22bd3fe7b4f39607",
                    "random_bit": false,
                    "accumulated_seed": "a0e424710f4fba036ba450b40f2bd7a842b176cf136f3af1952a2a13eb02616c",
                    "era_end": {
                        "equivocators": [
                            "01cc718e9dea652577bffad3471d0db7d03ba30923780a2a8fd1e3dd9b4e72dc54",
                            "0203e4532e401326892aa8ebc16b6986bd35a6c96a1f16c28db67fd7e87cb6913817",
                            "020318a52d5b2d545def8bf0ee5ea7ddea52f1fbf106c8b69848e40c5460e20c9f62"
                        ],
                        "inactive_validators": ["01cc718e9dea652577bffad3471d0db7d03ba30923780a2a8fd1e3dd9b4e72dc55", "01cc718e9dea652577bffad3471d0db7d03ba30923780a2a8fd1e3dd9b4e72dc56"],
                        "next_era_validator_weights": [
                            {"validator": "02038b238d774c3c4228a0430e3a078e1a2533f9c87cccbcf695637502d8d6057a63", "weight": "1"},
                            {"validator": "0102ffd4d2812d68c928712edd012fbcad54367bc6c5c254db22cf696772856566", "weight": "2"}
                        ],
                        "rewards": {
                            "02028b18c949d849b377988ea5191b39340975db25f8b80f37cc829c9f79dbfb19fc": ["749546792"],
                            "02028002c063228ff4e9d22d69154c499b86a4f7fdbf1d1e20f168b62da537af64c2": ["788342677"],
                            "02038efa405f648c72f36b0e5f37db69ab213d44404591b24de21383d8cc161101ec": ["86241635"],
                            "01f6bbd4a6fd10534290c58edb6090723d481cea444a8e8f70458e5136ea8c733c": ["941794198"]
                        },
                        "next_era_gas_price": 1
                    },
                    "timestamp": "2024-04-25T20:31:39.895Z",
                    "era_id": 419571,
                    "height": 4195710,
                    "protocol_version": "1.0.0",
                    "current_gas_price": 1
                },
                "body": {
                    "transactions": {
                        "0": [{"Deploy": "58aca0009fc41bd045d303db9e9f07416ff1fd8c76ecd98545eedf86f9459e80"}, {"Deploy": "58aca0009fc41bd045d303db9e9f07416ff1fd8c76ecd98545eedf86f9459e81"}, {"Version1": "58aca0009fc41bd045d303db9e9f07416ff1fd8c76ecd98545eedf86f9459e91"}],
                        "1": [{"Deploy": "58aca0009fc41bd045d303db9e9f07416ff1fd8c76ecd98545eedf86f9459e80"}, {"Deploy": "58aca0009fc41bd045d303db9e9f07416ff1fd8c76ecd98545eedf86f9459e90"}, {"Version1": "58aca0009fc41bd045d303db9e9f07416ff1fd8c76ecd98545eedf86f9459e82"}],
                        "2": [{"Deploy": "58aca0009fc41bd045d303db9e9f07416ff1fd8c76ecd98545eedf86f9459e83"}, {"Deploy": "58aca0009fc41bd045d303db9e9f07416ff1fd8c76ecd98545eedf86f9459e84"}, {"Version1": "58aca0009fc41bd045d303db9e9f07416ff1fd8c76ecd98545eedf86f9459e85"}],
                        "3": [{"Deploy": "58aca0009fc41bd045d303db9e9f07416ff1fd8c76ecd98545eedf86f9459e89"}, {"Deploy": "58aca0009fc41bd045d303db9e9f07416ff1fd8c76ecd98545eedf86f9459e90"}, {"Version1": "58aca0009fc41bd045d303db9e9f07416ff1fd8c76ecd98545eedf86f9459e88"}]
                    },
                    "rewarded_signatures": [[240],[0],[0]]
                }
            }
        }
    }
}"#;

const RAW_LEGACY_BLOCK_ADDED_FROM_V2: &str = r#"{
    "BlockAdded": {
        "block_hash": "2df9fb8909443fba928ed0536a79780cdb4557d0c05fdf762a1fd61141121422",
        "block": {
            "hash": "2df9fb8909443fba928ed0536a79780cdb4557d0c05fdf762a1fd61141121422",
            "header": {
                "parent_hash": "b8f5e9afd2e54856aa1656f962d07158f0fdf9cfac0f9992875f31f6bf2623a2",
                "state_root_hash": "cbf02d08bb263aa8915507c172b5f590bbddcd68693fb1c71758b5684b011730",
                "body_hash": "6041ab862a1e14a43a8e8a9a42dad27091915a337d18060c22bd3fe7b4f39607",
                "random_bit": false,
                "accumulated_seed": "a0e424710f4fba036ba450b40f2bd7a842b176cf136f3af1952a2a13eb02616c",
                "era_end": {
                    "era_report": {
                        "equivocators": [
                            "01cc718e9dea652577bffad3471d0db7d03ba30923780a2a8fd1e3dd9b4e72dc54",
                            "0203e4532e401326892aa8ebc16b6986bd35a6c96a1f16c28db67fd7e87cb6913817",
                            "020318a52d5b2d545def8bf0ee5ea7ddea52f1fbf106c8b69848e40c5460e20c9f62"
                        ],
                        "rewards": [
                            {
                                "validator": "01f6bbd4a6fd10534290c58edb6090723d481cea444a8e8f70458e5136ea8c733c",
                                "amount": 941794198
                            },
                            {
                                "validator": "02028002c063228ff4e9d22d69154c499b86a4f7fdbf1d1e20f168b62da537af64c2",
                                "amount": 788342677
                            },
                            {
                                "validator": "02028b18c949d849b377988ea5191b39340975db25f8b80f37cc829c9f79dbfb19fc",
                                "amount": 749546792
                            },
                            {
                                "validator": "02038efa405f648c72f36b0e5f37db69ab213d44404591b24de21383d8cc161101ec",
                                "amount": 86241635
                            }
                        ],
                        "inactive_validators": [
                            "01cc718e9dea652577bffad3471d0db7d03ba30923780a2a8fd1e3dd9b4e72dc55",
                            "01cc718e9dea652577bffad3471d0db7d03ba30923780a2a8fd1e3dd9b4e72dc56"
                        ]
                    },
                    "next_era_validator_weights": [
                        {
                            "validator": "0102ffd4d2812d68c928712edd012fbcad54367bc6c5c254db22cf696772856566",
                            "weight": "2"
                        },
                        {
                            "validator": "02038b238d774c3c4228a0430e3a078e1a2533f9c87cccbcf695637502d8d6057a63",
                            "weight": "1"
                        }
                    ]
                },
                "timestamp": "2024-04-25T20:31:39.895Z",
                "era_id": 419571,
                "height": 4195710,
                "protocol_version": "1.0.0"
            },
            "body": {
                "proposer": "01d3eec0445635f136ae560b43e9d8f656a6ba925f01293eaf2610b39ebe0fc28d",
                "deploy_hashes": [
                    "58aca0009fc41bd045d303db9e9f07416ff1fd8c76ecd98545eedf86f9459e89",
                    "58aca0009fc41bd045d303db9e9f07416ff1fd8c76ecd98545eedf86f9459e90"
                ],
                "transfer_hashes": [
                    "58aca0009fc41bd045d303db9e9f07416ff1fd8c76ecd98545eedf86f9459e80",
                    "58aca0009fc41bd045d303db9e9f07416ff1fd8c76ecd98545eedf86f9459e81"
                ]
            }
        }
    }
}"#;

const RAW_DEPLOY_PROCESSED: &str = r#"{
    "TransactionProcessed": {
        "transaction_hash": {
            "Deploy": "1660c3971ba4283583e4abc1cbef5ea0845a37300ae40a8728ae2684c1b1a5d2"
        },
        "initiator_addr": {
            "PublicKey": "0203d98e2ec694b981cebd7cf35ac531d8717e86dc35912f2536b8807ad550621147"
        },
        "timestamp": "2020-08-07T01:24:27.283Z",
        "ttl": "4h 11m 524ms",
        "block_hash": "e91cd454e94f2e3d155fa71105251cd0905e91067872a743d5d22974caabab06",
        "execution_result": {
            "Version2": {
                "initiator": {
                    "PublicKey": "0203d98e2ec694b981cebd7cf35ac531d8717e86dc35912f2536b8807ad550621147"
                },
                "error_message": "Error message 18290057561582514745",
                "limit": "11209375253254652626",
                "consumed": "10059559442643035623",
                "cost": "44837501013018610504",
                "payment": [
                    {
                        "source": "uref-da6b7bf686013e620f7efd057bb0285ab512324b5e69be0f16691fd9c6acb4e4-005"
                    }
                ],
                "transfers": [],
                "effects": [],
                "size_estimate": 521
            }
        },
        "messages": [
            {
                "entity_hash": "entity-system-fbd35eaf71f295b3bf35a295e705f629bbea28cefedfc109eda1205fb3650bad",
                "message": {
                    "String": "cs5rHI2Il75nRJ7GLs7BQM5CilvzMqu0dgFuj57FkqEs3431LJ1qfsZActb05hzR"
                },
                "topic_name": "7DnsHE3NL4PRaYuPcY90bECdnd7D78lF",
                "topic_name_hash": "f75840ed75ad1c85856de00d2ca865a7608b46a933d81c64ff8907ec620d6e83",
                "topic_index": 2222189259,
                "block_index": 11650550294672125610
            }
        ]
    }
}"#;

const RAW_LEGACY_DEPLOY_PROCESSED: &str = r#"{
    "DeployProcessed": {
        "deploy_hash": "1660c3971ba4283583e4abc1cbef5ea0845a37300ae40a8728ae2684c1b1a5d2",
        "account": "0203d98e2ec694b981cebd7cf35ac531d8717e86dc35912f2536b8807ad550621147",
        "timestamp": "2020-08-07T01:24:27.283Z",
        "ttl": "4h 11m 524ms",
        "dependencies": [],
        "block_hash": "e91cd454e94f2e3d155fa71105251cd0905e91067872a743d5d22974caabab06",
        "execution_result": {
            "Failure": {
                "effect": {
                    "operations": [],
                    "transforms": []
                },
                "transfers": [],
                "cost": "44837501013018610504",
                "error_message": "Error message 18290057561582514745"
            }
        }
    }
}"#;
