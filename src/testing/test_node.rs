use std::convert::Infallible;
use std::net::SocketAddr;
use std::time::Duration;
use std::vec::IntoIter;
use casper_types::ProtocolVersion;
use casper_types::testing::TestRng;
use futures_util::{stream, StreamExt, TryStreamExt};
use futures_util::stream::{Map, Zip, Iter, IntoStream};
use sse_client::EventSource;
use tokio::time::{Instant, interval};
use tokio::sync::oneshot;
use tokio_stream::wrappers::IntervalStream;
use warp::{sse::Event, Filter};
use crate::SseData;

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
const NUM_OF_TEST_EVENTS: usize = 8;
const TEST_API_VERSION: ProtocolVersion = ProtocolVersion::V1_0_0;

fn enclose_events_data(data: &mut Vec<SseData>, add_shutdown: bool) -> Vec<SseData> {
    data.insert(0, SseData::ApiVersion(TEST_API_VERSION));
    if add_shutdown {
        data.insert(data.len() - 1, SseData::Shutdown);
    }
    data.to_owned()
}

pub async fn start_test_node(port: u16, shutdown_receiver: oneshot::Receiver<()>) -> SocketAddr {

    let mut rng = TestRng::new();

    let mut blocks_data: Vec<SseData> =
        (1..=NUM_OF_TEST_EVENTS).map(|_| {
        SseData::random_block_added(&mut rng)
    }).collect();
    blocks_data = enclose_events_data(&mut blocks_data, false);

    let mut deploys_data: Vec<SseData> =
        (1..=NUM_OF_TEST_EVENTS).map(|_| {
            SseData::random_deploy_processed(&mut rng)
        }).collect();
    deploys_data = enclose_events_data(&mut deploys_data, false);

    let mut sigs_data: Vec<SseData> =
        (1..=NUM_OF_TEST_EVENTS).map(|_| {
            SseData::random_finality_signature(&mut rng)
        }).collect();
    sigs_data = enclose_events_data(&mut sigs_data, true);

    let event_root = warp::path("events");

    let main_channel = event_root.and(warp::path("main")).and(warp::path::end()).map(move || {
        println!("Checking Main Channel");

        let cloned_data = blocks_data.clone();

        let mut counter: u8 = 0;
        let interval = interval(Duration::from_millis(500));
        let timer_stream = IntervalStream::new(interval);
        let data_stream = stream::iter(cloned_data);
        let combined_stream = timer_stream.zip(data_stream);

        let event_stream = combined_stream.map(move |(_, data)| {
            println!("Event: Main");
            let event = match counter {
                0 => Event::default().json_data(data).expect("Error building Event"),
                _ => Event::default().id(counter.to_string()).json_data(data).expect("Error building Event"),
            };
            counter += 1;
            Result::<Event, Infallible>::Ok(event)
        });

        warp::sse::reply(event_stream)
    });

    let deploys_channel = event_root.and(warp::path("deploys")).and(warp::path::end()).map(move || {
        println!("Checking Deploys Channel");

        let cloned_data = deploys_data.clone();

        let mut counter: u8 = 0;
        let interval = interval(Duration::from_millis(500));
        let timer_stream = IntervalStream::new(interval);
        let data_stream = stream::iter(cloned_data);
        let combined_stream = timer_stream.zip(data_stream);

        let event_stream = combined_stream.map(move |(_, data)| {
            println!("Event: Deploy");
            let event = match counter {
                0 => Event::default().json_data(data).expect("Error building Event"),
                _ => Event::default().id(counter.to_string()).json_data(data).expect("Error building Event"),
            };
            counter += 1;
            Result::<Event, Infallible>::Ok(event)
        });

        warp::sse::reply(event_stream)
    });

    let sigs_channel = event_root.and(warp::path("sigs")).and(warp::path::end()).map(move || {
        println!("Checking Sigs Channel");

        let cloned_data = sigs_data.clone();

        let mut counter: u8 = 0;
        let interval = interval(Duration::from_millis(500));
        let timer_stream = IntervalStream::new(interval);
        let data_stream = stream::iter(cloned_data);
        let combined_stream = timer_stream.zip(data_stream);

        let event_stream = combined_stream.map(move |(_, data)| {
            println!("Event: Sig");
            let event = match counter {
                0 => Event::default().json_data(data).expect("Error building Event"),
                _ => Event::default().id(counter.to_string()).json_data(data).expect("Error building Event"),
            };
            counter += 1;
            Result::<Event, Infallible>::Ok(event)
        });

        warp::sse::reply(event_stream)
    });

    let routes = main_channel.or(deploys_channel).or(sigs_channel);

    let (addr, server) = warp::serve(routes).bind_with_graceful_shutdown(([127,0,0,1],port), async {
        shutdown_receiver.await.ok();
        println!("Test Node shutting down");
    });

    tokio::spawn(async {
        println!("Test Node starting...");
        server.await
    });

    return addr;
}

#[tokio::test(flavor="multi_thread", worker_threads=2)]
async fn check_node_output() {

    let test_node_port: u16 = 4444;

    let (node_shutdown_tx, node_shutdown_rx) = oneshot::channel();

    tokio::spawn(start_test_node(test_node_port, node_shutdown_rx));

    // Allows server to boot up
    // todo this method is brittle should really have a concrete and dynamic method for determining liveness of the server
    tokio::time::sleep(Duration::from_secs(1)).await;

    let url = format!("http://127.0.0.1:{}/events/{}", test_node_port, "main");

    let sse_stream = EventSource::new(&url);
    assert!(sse_stream.is_ok());
    for (index, evt) in sse_stream.unwrap().receiver().iter().enumerate() {
        let sse_data = serde_json::from_str::<SseData>(&evt.data).expect("Error parsing event data");
        if index == 0 {
            check_api_version_event(evt);
        } else {
            assert!(matches!(sse_data, SseData::BlockAdded {..}))
        }
        if index == NUM_OF_TEST_EVENTS { break; }
    };

    let url = format!("http://127.0.0.1:{}/events/{}", test_node_port, "deploys");

    let sse_stream = EventSource::new(&url);
    assert!(sse_stream.is_ok());
    for (index, evt) in sse_stream.unwrap().receiver().iter().enumerate() {
        let sse_data = serde_json::from_str::<SseData>(&evt.data).expect("Error parsing event data");
        if index == 0 {
            check_api_version_event(evt);
        } else {
            assert!(matches!(sse_data, SseData::DeployProcessed {..}))
        }
        if index == NUM_OF_TEST_EVENTS { break; }
    };

    let url = format!("http://127.0.0.1:{}/events/{}", test_node_port, "sigs");

    let sse_stream = EventSource::new(&url);
    assert!(sse_stream.is_ok());
    for (index, evt) in sse_stream.unwrap().receiver().iter().enumerate() {
        let sse_data = serde_json::from_str::<SseData>(&evt.data).expect("Error parsing event data");
        if index == 0 {
            check_api_version_event(evt);
        } else {
            assert!(matches!(sse_data, SseData::FinalitySignature {..}))
        }
        if index == NUM_OF_TEST_EVENTS { break; }
    };

    let _ = node_shutdown_tx.send(());
}

fn check_api_version_event(event: sse_client::Event) {
    assert!(event.id.is_empty());
    let data = serde_json::from_str::<SseData>(&event.data).expect("Error parsing API event");
    assert!(matches!(data, SseData::ApiVersion(..)));
}