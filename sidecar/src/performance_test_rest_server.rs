use std::time::Instant;

use super::*;
use crate::{
    rest_server::{
        filters,
        tests_helpers::{
            build_list_deploys_request_limit_offset, deserialize_deploys, list_deploys_raw,
            populate_with_blocks_and_deploys,
        },
    },
    types::config::SqliteConfig,
};
use casper_types::testing::TestRng;

const MAX_CONNECTIONS: u32 = 10;

//Given 1000 deploys when listing deploys (sorted) in batches per 100 then
//average of requests should be below 50ms
#[tokio::test(flavor = "multi_thread", worker_threads = 5)]
#[ignore]
async fn given_1000_deploys_when_listing_deploys_sorted_per_100_then_should_be() {
    let mut test_rng = TestRng::new();
    let database = SqliteDatabase::new_in_memory(MAX_CONNECTIONS)
        .await
        .expect("Error opening database in memory");
    let (_, _, _) = populate_with_blocks_and_deploys(&mut test_rng, &database, 10, 100, None).await;
    let api = filters::combined_filters(database);
    let mut test_duration = Duration::new(0, 0);
    let n = 10;
    for i in 0..n {
        let list_request = build_list_deploys_request_limit_offset(Some(100), Some(i * 100));
        let start = Instant::now();
        let response = list_deploys_raw(&api, list_request).await;
        test_duration = test_duration + start.elapsed();
        let page = deserialize_deploys(response);
        assert_eq!(page.item_count, 1000);
        assert_eq!(page.data.len(), 100);
    }
    let time_of_one_request = test_duration / n;
    assert!(time_of_one_request < Duration::from_millis(20));
}

async fn x(
    mut test_rng: TestRng,
    database: &SqliteDatabase,
    event_id_base: u32,
) -> (TestRng, u32) {
    let number_of_blocks: u32 = 100;
    let deploys_per_block: u32 = 100;
    let (_, _, _) = populate_with_blocks_and_deploys(
        &mut test_rng,
        database,
        number_of_blocks,
        deploys_per_block,
        Some(event_id_base),
    )
    .await;
    (test_rng, number_of_blocks * deploys_per_block)
}
//Given 10000 deploys when listing deploys (sorted) in batches per 100 then
//average of requests should be below 200ms
#[tokio::test(flavor = "multi_thread", worker_threads = 5)]
#[ignore]
async fn given_10000_deploys_when_listing_deploys_sorted_per_100_then_should_be() {
    let mut test_rng = TestRng::new();
    let p = Path::new("/home/jz/DEV/sources/storage");
    let config = SqliteConfig {
        file_name: String::from("sqlite_database.db3"),
        max_connections_in_pool: 10,
        wal_autocheckpointing_interval: 1000,
    };
    let database = SqliteDatabase::new(p, config)
        .await
        .expect("Error opening database in memory");
    let api = filters::combined_filters(database);
    let mut test_duration = Duration::new(0, 0);
    let n = 100;
    for i in 0..n {
        let list_request = build_list_deploys_request_limit_offset(Some(100), Some(i * 100));
        let start = Instant::now();
        let response = list_deploys_raw(&api, list_request).await;
        let one_elapsed = start.elapsed();
        test_duration = test_duration + one_elapsed;
        let page = deserialize_deploys(response);
        //assert_eq!(page.item_count, 10000);
        assert_eq!(page.data.len(), 100);
    }
    let time_of_one_request = test_duration / n;
    println!("Single request avg time: {:?}", time_of_one_request);
    assert!(time_of_one_request < Duration::from_millis(40));
}
