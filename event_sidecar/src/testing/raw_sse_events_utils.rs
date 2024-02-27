#[cfg(test)]
pub(crate) mod tests {
    use crate::testing::simple_sse_server::tests::{CacheAndData, SimpleSseServer};
    use casper_event_types::sse_data::test_support::*;
    use casper_event_types::sse_data::SseData;
    use casper_types::testing::TestRng;
    use hex_fmt::HexFmt;
    use std::collections::HashMap;
    use tokio::sync::mpsc::{Receiver, Sender};

    pub type EventsWithIds = Vec<(Option<String>, String)>;

    pub fn example_data_2_0_1() -> EventsWithIds {
        vec![
            (None, "{\"ApiVersion\":\"2.0.1\"}".to_string()),
            (
                Some("0".to_string()),
                example_block_added_2_0_0(BLOCK_HASH_3, "3"),
            ),
        ]
    }

    pub fn sse_server_shutdown_2_0_0_data() -> EventsWithIds {
        vec![
            (None, "{\"ApiVersion\":\"2.0.0\"}".to_string()),
            (Some("0".to_string()), shutdown()),
            (
                Some("1".to_string()),
                example_block_added_2_0_0(BLOCK_HASH_1, "1"),
            ),
        ]
    }

    pub fn random_n_block_added(
        number_of_block_added_messages: u32,
        start_index: u32,
        rng: TestRng,
    ) -> (EventsWithIds, TestRng) {
        let (blocks_added, rng) =
            generate_random_blocks_added(number_of_block_added_messages, start_index, rng);
        let data = vec![(None, "{\"ApiVersion\":\"2.0.0\"}".to_string())];
        let mut data: EventsWithIds = data.into_iter().chain(blocks_added).collect();
        let shutdown_index: u32 = start_index + 31;
        data.push((Some(shutdown_index.to_string()), shutdown()));
        (data, rng)
    }

    pub fn sse_server_example_data(version: &str) -> EventsWithIds {
        vec![
            (None, format!("{{\"ApiVersion\":\"{version}\"}}")),
            (
                Some("1".to_string()),
                example_block_added_2_0_0(BLOCK_HASH_2, "2"),
            ),
        ]
    }

    pub fn sse_server_example_2_0_0_data() -> EventsWithIds {
        vec![
            (None, "{\"ApiVersion\":\"2.0.0\"}".to_string()),
            (
                Some("1".to_string()),
                example_block_added_2_0_0(BLOCK_HASH_2, "2"),
            ),
        ]
    }

    pub fn sse_server_example_2_0_0_data_second() -> EventsWithIds {
        vec![
            (None, "{\"ApiVersion\":\"2.0.0\"}".to_string()),
            (
                Some("3".to_string()),
                example_block_added_2_0_0(BLOCK_HASH_3, "3"),
            ),
        ]
    }

    pub fn sse_server_example_2_0_0_data_third() -> EventsWithIds {
        vec![
            (None, "{\"ApiVersion\":\"2.0.0\"}".to_string()),
            (
                Some("1".to_string()),
                example_block_added_2_0_0(BLOCK_HASH_3, "3"),
            ),
            (
                Some("1".to_string()),
                example_block_added_2_0_0(BLOCK_HASH_4, "4"),
            ),
        ]
    }

    pub async fn simple_sse_server(
        port: u16,
        data: EventsWithIds,
        cache: EventsWithIds,
    ) -> (Sender<()>, Receiver<()>, Vec<String>) {
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
        (shutdown_tx, after_shutdown_rx, urls)
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
                    example_block_added_2_0_0(encoded_hash.as_str(), index.as_str());
                blocks_added.push((Some(index), block_added_raw));
            } else {
                panic!("random_block_added didn't return SseData::BlockAdded");
            }
        }
        (blocks_added, rng)
    }
}
