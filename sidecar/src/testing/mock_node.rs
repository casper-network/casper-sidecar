use std::convert::Infallible;
use std::net::SocketAddr;
use std::time::Duration;

use casper_event_types::SseData;
use casper_types::{testing::TestRng, ProtocolVersion};

#[allow(unused)]
use eventsource_stream::{EventStream, Eventsource};
#[allow(unused)]
use futures::{Stream, StreamExt};
use futures_util::stream;
#[allow(unused)]
use futures_util::stream::{IntoStream, Iter, Map, Zip};
use rand::Rng;
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

async fn start_mock_node(
    port: u16,
    started_notification_sender: oneshot::Sender<()>,
    shutdown_receiver: oneshot::Receiver<()>,
    num_events: usize,
) -> SocketAddr {
    let mut rng = TestRng::new();

    // todo add other MAIN filter events
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

/// Starts the mock node and returns the port it's running on as well as the oneshot sender to shut it down at the end of the test.
pub(crate) async fn start_mock_node_with_shutdown(
    num_events: Option<usize>,
) -> (u16, oneshot::Sender<()>) {
    let (node_shutdown_tx, node_shutdown_rx) = oneshot::channel();
    let (node_started_tx, node_started_rx) = oneshot::channel();

    let port = portpicker::pick_unused_port().expect("Unable to get free port");

    tokio::spawn(start_mock_node(
        port,
        node_started_tx,
        node_shutdown_rx,
        num_events.unwrap_or(DEFAULT_NUM_OF_TEST_EVENTS),
    ));

    // Wait for the test node to report that it's live
    let _ = node_started_rx.await;

    (port, node_shutdown_tx)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn main_filter_should_provide_valid_data() {
    filter_should_provide_valid_data("main").await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deploys_filter_should_provide_valid_data() {
    filter_should_provide_valid_data("deploys").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sigs_filter_should_provide_valid_data() {
    filter_should_provide_valid_data("sigs").await;
}

#[cfg(test)]
async fn filter_should_provide_valid_data(filter: &str) {
    let mut test_rng = TestRng::new();
    let num_events = test_rng.gen_range(3..10);

    let (port, node_shutdown_tx) = start_mock_node_with_shutdown(Some(num_events)).await;

    let mock_node_url = format!("http://127.0.0.1:{}/events/{}", port, filter);
    let mut connection = reqwest::Client::new()
        .get(&mock_node_url)
        .send()
        .await
        .expect("Error connecting to event stream")
        .bytes_stream()
        .eventsource();

    let mut event_count = 0;

    while let Some(event) = connection.next().await {
        serde_json::from_str::<SseData>(
            &event
                .expect("Event was an error from the event stream")
                .data,
        )
        .expect("Error deserialising the event into SseData");
        event_count += 1;
    }

    assert_eq!(event_count, num_events + 2);

    let _ = node_shutdown_tx.send(());
}
