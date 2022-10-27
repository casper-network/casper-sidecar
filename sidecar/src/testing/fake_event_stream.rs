use std::time::Duration;

use casper_event_types::SseData;
use casper_types::{testing::TestRng, ProtocolVersion};

use tempfile::TempDir;
use tokio::sync::mpsc::unbounded_channel;
use tokio::time::Instant;

use crate::event_stream_server::{Config as SseConfig, EventStreamServer};

const TIME_BETWEEN_BLOCKS_IN_SECONDS: u64 = 4;

type FrequencyOfStepEvents = u8;
type NumberOfDeployEventsInBurst = u8;

pub enum EventStreamScenario {
    Realistic,
    LoadTestingStep(FrequencyOfStepEvents),
    LoadTestingDeploy(NumberOfDeployEventsInBurst),
}

pub async fn spin_up_fake_event_stream(
    port: u16,
    scenario: EventStreamScenario,
    duration_in_seconds: u64,
) {
    let start = Instant::now();

    let temp_dir = TempDir::new().expect("Error creating temporary directory");

    let event_stream_server = EventStreamServer::new(
        SseConfig::new(port, None, None),
        temp_dir.path().to_path_buf(),
        ProtocolVersion::V1_0_0,
    )
    .expect("Error spinning up Event Stream Server");

    match scenario {
        EventStreamScenario::Realistic => {
            realistic_event_streaming(event_stream_server, duration_in_seconds).await
        }
        EventStreamScenario::LoadTestingStep(frequency) => {
            repeat_step_events(event_stream_server, duration_in_seconds, frequency).await
        }
        EventStreamScenario::LoadTestingDeploy(num_in_burst) => {
            fast_bursts_of_deploy_events(event_stream_server, duration_in_seconds, num_in_burst)
                .await;
        }
    }

    println!(
        "Fake Event Stream(:{}) shutdown after {}s",
        port,
        start.elapsed().as_secs()
    );
}

async fn realistic_event_streaming(mut server: EventStreamServer, stream_duration_in_secs: u64) {
    let (broadcast_sender, mut broadcast_receiver) = unbounded_channel();

    let cloned_sender = broadcast_sender.clone();

    let frequent_handle = tokio::spawn(tokio::time::timeout(
        Duration::from_secs(stream_duration_in_secs),
        async move {
            let mut test_rng = TestRng::new();

            let mut interval =
                tokio::time::interval(Duration::from_secs(TIME_BETWEEN_BLOCKS_IN_SECONDS));

            loop {
                // Four Block cycles
                for _ in 0..4 {
                    interval.tick().await;

                    // Prior to each BlockAdded emit FinalitySignatures
                    for _ in 0..4 {
                        let _ =
                            cloned_sender.send(SseData::random_finality_signature(&mut test_rng));
                    }

                    // Emit DeployProcessed events for the next BlockAdded
                    for _ in 0..4 {
                        let _ = cloned_sender.send(SseData::random_deploy_processed(&mut test_rng));
                    }

                    // Emit the BlockAdded
                    let _ = cloned_sender.send(SseData::random_block_added(&mut test_rng));

                    // Emit DeployAccepted Events
                    for _ in 0..4 {
                        let _ =
                            cloned_sender.send(SseData::random_deploy_accepted(&mut test_rng).0);
                    }
                }
                // Then a Step
                let _ = cloned_sender.send(SseData::random_step(&mut test_rng));
            }
        },
    ));

    let infrequent_handle = tokio::spawn(tokio::time::timeout(
        Duration::from_secs(stream_duration_in_secs),
        async move {
            let mut test_rng = TestRng::new();
            loop {
                tokio::time::sleep(Duration::from_secs(stream_duration_in_secs / 4)).await;
                let _ = broadcast_sender.send(SseData::random_deploy_expired(&mut test_rng));
                tokio::time::sleep(Duration::from_secs(stream_duration_in_secs / 2)).await;
                let _ = broadcast_sender.send(SseData::random_fault(&mut test_rng));
            }
        },
    ));

    let broadcast_handle = tokio::spawn(tokio::time::timeout(
        Duration::from_secs(stream_duration_in_secs),
        async move {
            while let Some(sse_data) = broadcast_receiver.recv().await {
                server.broadcast(sse_data);
            }
        },
    ));

    let _ = tokio::join!(frequent_handle, infrequent_handle, broadcast_handle);
}

async fn repeat_step_events(
    mut event_stream_server: EventStreamServer,
    duration_in_seconds: u64,
    frequency: u8,
) {
    let mut test_rng = TestRng::new();

    let start_time = Instant::now();

    while start_time.elapsed() < Duration::from_secs(duration_in_seconds) {
        event_stream_server.broadcast(SseData::random_step(&mut test_rng));
        tokio::time::sleep(Duration::from_millis(1000 / frequency as u64)).await;
    }
}

async fn fast_bursts_of_deploy_events(
    mut event_stream_server: EventStreamServer,
    duration_in_seconds: u64,
    burst_size: u8,
) {
    let mut test_rng = TestRng::new();

    let start_time = Instant::now();

    while start_time.elapsed() < Duration::from_secs(duration_in_seconds) {
        for _ in 0..burst_size {
            event_stream_server.broadcast(SseData::random_deploy_accepted(&mut test_rng).0);
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
        for _ in 0..burst_size {
            event_stream_server.broadcast(SseData::random_deploy_processed(&mut test_rng));
        }
    }
}
