use std::convert::Infallible;
use std::net::SocketAddr;
use std::time::Duration;
// use std::vec::IntoIter;
use crate::SseData;
use casper_types::testing::TestRng;
use casper_types::ProtocolVersion;
#[allow(unused)]
use eventsource_stream::{EventStream, Eventsource};
#[allow(unused)]
use futures::{Stream, StreamExt};
use futures_util::stream;
#[allow(unused)]
use futures_util::stream::{IntoStream, Iter, Map, Zip};
use serial_test::serial;
use tokio::sync::oneshot;
#[allow(unused)]
use tokio::time::{interval, Instant};
use tokio_stream::wrappers::IntervalStream;
use warp::{sse::Event, Filter};

// fn build_event<I>(id: Option<I>, data: SseData) -> Result<Event, Infallible>
//     where I: Into<String>,
// {
//     let event = match id {
//         None => Event::default().json_data(data).expect("Error building event without ID"),
//         Some(id) => Event::default().id(id).json_data(data).expect("Error building event")
//     };
//     Ok(event)
// }
//
// fn build_stream(data: Vec<SseData>) -> Map<Zip<IntervalStream, Iter<IntoIter<SseData>>>, fn((Instant, SseData)) -> Result<Event, Infallible>> {
//     let counter: u8 = 0;
//     let interval = interval(Duration::from_secs(1));
//     let timer_stream = IntervalStream::new(interval);
//     let data_stream = stream::iter(data);
//     let combined_stream = timer_stream.zip(data_stream);
//
//     combined_stream.map(build_event)
// }

// Number of events to send excluding the first (API) and last (Shutdown) events.
const DEFAULT_NUM_OF_TEST_EVENTS: usize = 8;
const TEST_API_VERSION: ProtocolVersion = ProtocolVersion::V1_0_0;

fn enclose_events_data(data: &mut Vec<SseData>, add_shutdown: bool) -> Vec<SseData> {
    data.insert(0, SseData::ApiVersion(TEST_API_VERSION));
    if add_shutdown {
        data.insert(data.len() - 1, SseData::Shutdown);
    }
    data.to_owned()
}

async fn start_test_node(
    port: u16,
    started_notification_sender: oneshot::Sender<()>,
    shutdown_receiver: oneshot::Receiver<()>,
    num_events: usize,
) -> SocketAddr {
    let mut rng = TestRng::new();

    let mut blocks_data: Vec<SseData> = (1..=num_events)
        .map(|_| SseData::random_block_added(&mut rng))
        .collect();
    blocks_data = enclose_events_data(&mut blocks_data, true);

    let mut deploys_data: Vec<SseData> = (1..=num_events)
        .map(|_| SseData::random_deploy_processed(&mut rng))
        .collect();
    deploys_data = enclose_events_data(&mut deploys_data, true);

    let mut sigs_data: Vec<SseData> = (1..=num_events)
        .map(|_| SseData::random_finality_signature(&mut rng))
        .collect();
    sigs_data = enclose_events_data(&mut sigs_data, true);

    let event_root = warp::path("events");

    let main_channel = event_root
        .and(warp::path("main"))
        .and(warp::path::end())
        .map(move || {
            let cloned_data = blocks_data.clone();

            let mut counter: u8 = 0;
            let interval = interval(Duration::from_millis(500));
            let timer_stream = IntervalStream::new(interval);
            let data_stream = stream::iter(cloned_data);
            let combined_stream = timer_stream.zip(data_stream);

            let event_stream = combined_stream.map(move |(_, data)| {
                let event = match counter {
                    0 => Event::default()
                        .json_data(data)
                        .expect("Error building Event"),
                    _ => Event::default()
                        .id(counter.to_string())
                        .json_data(data)
                        .expect("Error building Event"),
                };
                counter += 1;
                Result::<Event, Infallible>::Ok(event)
            });

            warp::sse::reply(event_stream)
        });

    let deploys_channel = event_root
        .and(warp::path("deploys"))
        .and(warp::path::end())
        .map(move || {
            let cloned_data = deploys_data.clone();

            let mut counter: u8 = 0;
            let interval = interval(Duration::from_millis(500));
            let timer_stream = IntervalStream::new(interval);
            let data_stream = stream::iter(cloned_data);
            let combined_stream = timer_stream.zip(data_stream);

            let event_stream = combined_stream.map(move |(_, data)| {
                let event = match counter {
                    0 => Event::default()
                        .json_data(data)
                        .expect("Error building Event"),
                    _ => Event::default()
                        .id(counter.to_string())
                        .json_data(data)
                        .expect("Error building Event"),
                };
                counter += 1;
                Result::<Event, Infallible>::Ok(event)
            });

            warp::sse::reply(event_stream)
        });

    let sigs_channel = event_root
        .and(warp::path("sigs"))
        .and(warp::path::end())
        .map(move || {
            let cloned_data = sigs_data.clone();

            let mut counter: u8 = 0;
            let interval = interval(Duration::from_millis(500));
            let timer_stream = IntervalStream::new(interval);
            let data_stream = stream::iter(cloned_data);
            let combined_stream = timer_stream.zip(data_stream);

            let event_stream = combined_stream.map(move |(_, data)| {
                let event = match counter {
                    0 => Event::default()
                        .json_data(data)
                        .expect("Error building Event"),
                    _ => Event::default()
                        .id(counter.to_string())
                        .json_data(data)
                        .expect("Error building Event"),
                };
                counter += 1;
                Result::<Event, Infallible>::Ok(event)
            });

            warp::sse::reply(event_stream)
        });

    let routes = main_channel.or(deploys_channel).or(sigs_channel);

    let (addr, server) =
        warp::serve(routes).bind_with_graceful_shutdown(([127, 0, 0, 1], port), async {
            shutdown_receiver.await.ok();
        });

    tokio::spawn(async {
        let _ = started_notification_sender.send(());
        server.await
    });

    addr
}

pub(crate) async fn start_test_node_with_shutdown(
    port: u16,
    num_events: Option<usize>,
) -> oneshot::Sender<()> {
    let (node_shutdown_tx, node_shutdown_rx) = oneshot::channel();
    let (node_started_tx, node_started_rx) = oneshot::channel();

    let num_events = num_events.unwrap_or(DEFAULT_NUM_OF_TEST_EVENTS);

    tokio::spawn(start_test_node(
        port,
        node_started_tx,
        node_shutdown_rx,
        num_events,
    ));

    // Wait for the test node to report that it's live
    let _ = node_started_rx.await;

    node_shutdown_tx
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn should_connect_then_gracefully_shutdown() {
    let test_node_port: u16 = 4444;

    let node_shutdown_tx = start_test_node_with_shutdown(test_node_port, None).await;

    let test_node_url = format!("http://127.0.0.1:{}/events/main", test_node_port);
    let mut connection = reqwest::Client::new()
        .get(&test_node_url)
        .send()
        .await
        .unwrap()
        .bytes_stream()
        .eventsource();

    let first_event = connection.next().await.unwrap().unwrap();
    assert!(first_event.data.contains("ApiVersion"));

    let _ = node_shutdown_tx.send(());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn main_filter_should_provide_valid_data() {
    let test_node_port: u16 = 4444;

    let node_shutdown_tx = start_test_node_with_shutdown(test_node_port, None).await;

    let test_node_url = format!("http://127.0.0.1:{}/events/main", test_node_port);
    let mut connection = reqwest::Client::new()
        .get(&test_node_url)
        .send()
        .await
        .unwrap()
        .bytes_stream()
        .eventsource();

    while let Some(event) = connection.next().await {
        let sse = serde_json::from_str::<SseData>(&event.unwrap().data).unwrap();
        if matches!(sse, SseData::Shutdown) {
            break;
        }
    }

    let _ = node_shutdown_tx.send(());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn deploys_filter_should_provide_valid_data() {
    let test_node_port: u16 = 4444;

    let node_shutdown_tx = start_test_node_with_shutdown(test_node_port, None).await;

    let test_node_url = format!("http://127.0.0.1:{}/events/deploys", test_node_port);
    let mut connection = reqwest::Client::new()
        .get(&test_node_url)
        .send()
        .await
        .unwrap()
        .bytes_stream()
        .eventsource();

    while let Some(event) = connection.next().await {
        let sse = serde_json::from_str::<SseData>(&event.unwrap().data).unwrap();
        if matches!(sse, SseData::Shutdown) {
            break;
        }
    }

    let _ = node_shutdown_tx.send(());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn sigs_filter_should_provide_valid_data() {
    let test_node_port: u16 = 4444;

    let node_shutdown_tx = start_test_node_with_shutdown(test_node_port, None).await;

    let test_node_url = format!("http://127.0.0.1:{}/events/sigs", test_node_port);
    let mut connection = reqwest::Client::new()
        .get(&test_node_url)
        .send()
        .await
        .unwrap()
        .bytes_stream()
        .eventsource();

    while let Some(event) = connection.next().await {
        let sse = serde_json::from_str::<SseData>(&event.unwrap().data).unwrap();
        if matches!(sse, SseData::Shutdown) {
            break;
        }
    }

    let _ = node_shutdown_tx.send(());
}
