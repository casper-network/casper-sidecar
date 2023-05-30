use std::time::Instant;

use super::*;
use crate::rest_server::{
    filters,
    tests_helpers::{
        build_list_deploys_request_limit_offset,
        populate_with_blocks_and_deploys, list_deploys_raw, deserialize_deploys,
    },
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
    let (_, _, _) = populate_with_blocks_and_deploys(&mut test_rng, &database, 10, 100).await;
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
    assert!(time_of_one_request < Duration::from_millis(50));
}

//Given 10000 deploys when listing deploys (sorted) in batches per 100 then 
//average of requests should be below 200ms
#[tokio::test(flavor = "multi_thread", worker_threads = 5)]
#[ignore]
async fn given_10000_deploys_when_listing_deploys_sorted_per_100_then_should_be() {
    let mut test_rng = TestRng::new();
    let database = SqliteDatabase::new_in_memory(MAX_CONNECTIONS)
        .await
        .expect("Error opening database in memory");
    let (_, _, _) = populate_with_blocks_and_deploys(&mut test_rng, &database, 1, 1).await;
    let api = filters::combined_filters(database);
    let mut test_duration = Duration::new(0, 0);
    let n = 1;
    for i in 0..n {
        let list_request = build_list_deploys_request_limit_offset(Some(100), Some(i * 100));
        let start = Instant::now();
        let response = list_deploys_raw(&api, list_request).await;
        let one_elapsed = start.elapsed();
        test_duration = test_duration + one_elapsed;
        let page = deserialize_deploys(response);
        assert_eq!(page.item_count, 10000);
        assert_eq!(page.data.len(), 100);
    }
    let time_of_one_request = test_duration / n;
    println!("AAAA {:?}", time_of_one_request);
    assert!(time_of_one_request < Duration::from_millis(200));
}

