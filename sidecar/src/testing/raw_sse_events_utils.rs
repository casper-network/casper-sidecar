#[cfg(test)]
pub(crate) mod tests {
    use crate::testing::fake_event_stream::wait_for_sse_server_to_be_up;
    use crate::testing::simple_sse_server::tests::{CacheAndData, SimpleSseServer};
    use casper_event_types::sse_data::test_support::{
        example_block_added_1_4_10, example_finality_signature_1_4_10, write_unbounding_deploy,
        BLOCK_HASH_1, BLOCK_HASH_2, BLOCK_HASH_3,
    };
    use casper_event_types::sse_data::SseData;
    use casper_event_types::sse_data_1_0_0::test_support::{example_block_added_1_0_0, shutdown};
    use casper_types::testing::TestRng;
    use hex_fmt::HexFmt;
    use std::collections::HashMap;
    use tokio::sync::mpsc::{Receiver, Sender};

    pub type EventsWithIds = Vec<(Option<String>, String)>;
    pub fn example_data_1_0_0() -> EventsWithIds {
        vec![
            (None, "{\"ApiVersion\":\"1.0.0\"}".to_string()),
            (
                Some("0".to_string()),
                example_block_added_1_0_0(BLOCK_HASH_1, "1"),
            ),
        ]
    }

    pub fn data_1_5_1_with_write_unbounding() -> EventsWithIds {
        vec![
            (None, "{\"ApiVersion\":\"1.5.1\"}".to_string()),
            (Some("0".to_string()), write_unbounding_deploy()),
        ]
    }

    pub fn example_data_1_0_0_two_blocks() -> EventsWithIds {
        vec![
            (None, "{\"ApiVersion\":\"1.0.0\"}".to_string()),
            (
                Some("0".to_string()),
                example_block_added_1_0_0(BLOCK_HASH_1, "1"),
            ),
            (
                Some("1".to_string()),
                example_block_added_1_0_0(BLOCK_HASH_3, "2"),
            ),
        ]
    }

    pub fn sse_server_shutdown_1_0_0_data() -> EventsWithIds {
        vec![
            (None, "{\"ApiVersion\":\"1.0.0\"}".to_string()),
            (Some("0".to_string()), shutdown()),
            (
                Some("1".to_string()),
                example_block_added_1_0_0(BLOCK_HASH_1, "1"),
            ),
        ]
    }

    pub fn example_data_1_3_9_with_sigs() -> (EventsWithIds, EventsWithIds) {
        let main = vec![
            (None, "{\"ApiVersion\":\"1.3.9\"}".to_string()),
            (
                Some("1".to_string()),
                example_block_added_1_4_10(BLOCK_HASH_2, "2"), //1.3.9 should use 1.4.x compliant BlockAdded messages
            ),
        ];
        let sigs = vec![
            (None, "{\"ApiVersion\":\"1.3.9\"}".to_string()),
            (
                Some("2".to_string()),
                example_finality_signature_1_4_10(BLOCK_HASH_2), //1.3.9 should use 1.4.x compliant FinalitySingatures messages
            ),
        ];
        (main, sigs)
    }

    pub fn random_n_block_added(
        number_of_block_added_messages: u32,
        start_index: u32,
        rng: TestRng,
    ) -> (EventsWithIds, TestRng) {
        let (blocks_added, rng) =
            generate_random_blocks_added(number_of_block_added_messages, start_index, rng);
        let data = vec![(None, "{\"ApiVersion\":\"1.4.10\"}".to_string())];
        let mut data: EventsWithIds = data.into_iter().chain(blocks_added.into_iter()).collect();
        let shutdown_index: u32 = start_index + 31;
        data.push((Some(shutdown_index.to_string()), shutdown()));
        (data, rng)
    }

    pub fn sse_server_example_1_4_10_data() -> EventsWithIds {
        vec![
            (None, "{\"ApiVersion\":\"1.4.10\"}".to_string()),
            (
                Some("1".to_string()),
                example_block_added_1_4_10(BLOCK_HASH_2, "2"),
            ),
        ]
    }

    pub fn sse_server_example_1_4_10_data_other() -> EventsWithIds {
        vec![
            (None, "{\"ApiVersion\":\"1.4.10\"}".to_string()),
            (
                Some("1".to_string()),
                example_block_added_1_4_10(BLOCK_HASH_3, "3"),
            ),
        ]
    }

    pub fn example_data_1_1_0_with_legacy_message() -> EventsWithIds {
        vec![
            (None, "{\"ApiVersion\":\"1.1.0\"}".to_string()),
            (
                Some("1".to_string()),
                example_block_added_1_0_0(BLOCK_HASH_2, "3"),
            ),
        ]
    }

    pub async fn simple_sse_server(
        port: u16,
        data: EventsWithIds,
        cache: EventsWithIds,
    ) -> (Sender<()>, Receiver<()>) {
        let cache_and_data = CacheAndData { cache, data };
        let paths_and_data: HashMap<Vec<String>, CacheAndData> = HashMap::from([
            (
                vec!["events".to_string(), "main".to_string()],
                cache_and_data.clone(),
            ),
            (vec!["events".to_string()], cache_and_data),
        ]);
        let sse_server = SimpleSseServer {
            routes: paths_and_data,
        };
        let (shutdown_tx, after_shutdown_rx) = sse_server.serve(port).await;
        let urls: Vec<String> = vec![
            format!("http://127.0.0.1:{}/events/main", port),
            format!("http://127.0.0.1:{}/events", port),
        ];
        wait_for_sse_server_to_be_up(urls).await;
        (shutdown_tx, after_shutdown_rx)
    }

    pub async fn simple_sse_server_with_sigs(
        port: u16,
        main_data: EventsWithIds,
        main_cache: EventsWithIds,
        sigs_data: EventsWithIds,
        sigs_cache: EventsWithIds,
    ) -> (Sender<()>, Receiver<()>) {
        let main_cache_and_data = CacheAndData {
            cache: main_cache,
            data: main_data,
        };
        let sigs_cache_and_data = CacheAndData {
            cache: sigs_cache,
            data: sigs_data,
        };
        let paths_and_data: HashMap<Vec<String>, CacheAndData> = HashMap::from([
            (
                vec!["events".to_string(), "main".to_string()],
                main_cache_and_data,
            ),
            (
                vec!["events".to_string(), "sigs".to_string()],
                sigs_cache_and_data,
            ),
        ]);
        let sse_server = SimpleSseServer {
            routes: paths_and_data,
        };
        let (shutdown_tx, after_shutdown_rx) = sse_server.serve(port).await;
        let urls = vec![
            format!("http://127.0.0.1:{}/events/main", port),
            format!("http://127.0.0.1:{}/events/sigs", port),
        ];
        wait_for_sse_server_to_be_up(urls).await;
        (shutdown_tx, after_shutdown_rx)
    }

    fn generate_random_blocks_added(
        number_of_block_added_messages: u32,
        start_index: u32,
        mut rng: TestRng,
    ) -> (EventsWithIds, TestRng) {
        let mut blocks_added = Vec::new();
        for i in 0..number_of_block_added_messages {
            let index = (i + start_index).to_string();
            let block_added = SseData::random_block_added(&mut rng);
            if let SseData::BlockAdded { block_hash, .. } = block_added {
                let encoded_hash = HexFmt(block_hash.inner()).to_string();
                let block_added_raw =
                    example_block_added_1_4_10(encoded_hash.as_str(), index.as_str());
                blocks_added.push((Some(index), block_added_raw));
            } else {
                panic!("random_block_added didn't return SseData::BlockAdded");
            }
        }
        (blocks_added, rng)
    }
}
