use std::{
    fmt::{Display, Formatter},
    time::Duration,
};

use derive_new::new;
use tempfile::TempDir;
use tokio::sync::mpsc::unbounded_channel;
use tokio::time::Instant;

use casper_event_types::SseData;
use casper_types::{testing::TestRng, ProtocolVersion};

use crate::event_stream_server::{Config as EssConfig, EventStreamServer};

// Based on Mainnet cadence
const TIME_BETWEEN_BLOCKS_IN_SECONDS: u64 = 30;

type FrequencyOfStepEvents = u8;
type NumberOfDeployEventsInBurst = u8;

#[derive(new)]
pub struct FesRestartConfig {
    shutdown_after: Duration,
    restart_after: Duration,
}

pub enum EventStreamScenario {
    Realistic,
    LoadTestingStep(FrequencyOfStepEvents),
    LoadTestingDeploy(NumberOfDeployEventsInBurst),
    WithRestart(FesRestartConfig),
}

impl Display for EventStreamScenario {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            EventStreamScenario::Realistic => write!(f, "Realistic"),
            EventStreamScenario::LoadTestingStep(_) => {
                write!(f, "Load Testing [Step]")
            }
            EventStreamScenario::LoadTestingDeploy(_) => {
                write!(f, "Load Testing [Deploy]")
            }
            EventStreamScenario::WithRestart(_) => {
                write!(f, "With Restart")
            }
        }
    }
}

pub async fn spin_up_fake_event_stream(
    rng_seed: [u8; 16],
    ess_config: EssConfig,
    scenario: EventStreamScenario,
    duration: Duration,
) {
    let start = Instant::now();

    let temp_dir = TempDir::new().expect("Error creating temporary directory");

    let cloned_address = ess_config.address.clone();
    let port = cloned_address.split(':').collect::<Vec<&str>>()[1];

    let event_stream_server = EventStreamServer::new(
        ess_config.clone(),
        temp_dir.path().to_path_buf(),
        ProtocolVersion::V1_0_0,
    )
    .expect("Error spinning up Event Stream Server");

    match scenario {
        EventStreamScenario::Realistic => {
            realistic_event_streaming(rng_seed, event_stream_server, duration).await
        }
        EventStreamScenario::LoadTestingStep(frequency) => {
            repeat_step_events(rng_seed, event_stream_server, duration, frequency).await
        }
        EventStreamScenario::LoadTestingDeploy(num_in_burst) => {
            fast_bursts_of_deploy_events(rng_seed, event_stream_server, duration, num_in_burst)
                .await;
        }
        EventStreamScenario::WithRestart(FesRestartConfig {
            shutdown_after,
            restart_after,
        }) => {
            assert!(duration > shutdown_after + restart_after, "Total duration must be greater than the time to wait before shutdown combined with the time to wait before restart");
            realistic_event_streaming(rng_seed, event_stream_server, shutdown_after).await;

            tokio::time::sleep(restart_after).await;

            let event_stream_server = EventStreamServer::new(
                ess_config,
                temp_dir.path().to_path_buf(),
                ProtocolVersion::V1_0_0,
            )
            .expect("Error spinning up Event Stream Server");

            realistic_event_streaming(
                rng_seed,
                event_stream_server,
                duration - shutdown_after - restart_after,
            )
            .await;
        }
    }

    println!(
        "Fake Event Stream(:{}) :: Scenario: {} :: Completed ({}s)",
        port,
        scenario,
        start.elapsed().as_secs()
    );
}

async fn realistic_event_streaming(
    rng_seed: [u8; 16],
    mut server: EventStreamServer,
    duration: Duration,
) {
    let (broadcast_sender, mut broadcast_receiver) = unbounded_channel();

    let cloned_sender = broadcast_sender.clone();

    let frequent_handle = tokio::spawn(tokio::time::timeout(duration, async move {
        let mut test_rng = TestRng::from_seed(rng_seed);

        let mut interval =
            tokio::time::interval(Duration::from_secs(TIME_BETWEEN_BLOCKS_IN_SECONDS));

        loop {
            // Four Block cycles
            for _ in 0..4 {
                interval.tick().await;

                // Prior to each BlockAdded emit FinalitySignatures
                for _ in 0..100 {
                    let _ = cloned_sender.send(SseData::random_finality_signature(&mut test_rng));
                }

                // Emit DeployProcessed events for the next BlockAdded
                for _ in 0..20 {
                    let _ = cloned_sender.send(SseData::random_deploy_processed(&mut test_rng));
                }

                // Emit the BlockAdded
                let _ = cloned_sender.send(SseData::random_block_added(&mut test_rng));

                // Emit DeployAccepted Events
                for _ in 0..20 {
                    let _ = cloned_sender.send(SseData::random_deploy_accepted(&mut test_rng).0);
                }
            }
            // Then a Step
            let _ = cloned_sender.send(SseData::random_step(&mut test_rng));
        }
    }));

    let infrequent_handle = tokio::spawn(tokio::time::timeout(duration, async move {
        let mut test_rng = TestRng::from_seed(rng_seed);
        loop {
            tokio::time::sleep(duration / 4).await;
            let _ = broadcast_sender.send(SseData::random_deploy_expired(&mut test_rng));
            tokio::time::sleep(duration / 2).await;
            let _ = broadcast_sender.send(SseData::random_fault(&mut test_rng));
        }
    }));

    let broadcast_handle = tokio::spawn(tokio::time::timeout(duration, async move {
        while let Some(sse_data) = broadcast_receiver.recv().await {
            server.broadcast(sse_data);
        }
    }));

    let _ = tokio::join!(frequent_handle, infrequent_handle, broadcast_handle);
}

async fn repeat_step_events(
    rng_seed: [u8; 16],
    mut event_stream_server: EventStreamServer,
    duration: Duration,
    frequency: u8,
) {
    let mut test_rng = TestRng::from_seed(rng_seed);

    let start_time = Instant::now();

    while start_time.elapsed() < duration {
        event_stream_server.broadcast(SseData::random_step(&mut test_rng));
        tokio::time::sleep(Duration::from_millis(1000 / frequency as u64)).await;
    }
}

async fn fast_bursts_of_deploy_events(
    rng_seed: [u8; 16],
    mut event_stream_server: EventStreamServer,
    duration: Duration,
    burst_size: u8,
) {
    let mut test_rng = TestRng::from_seed(rng_seed);

    let start_time = Instant::now();

    while start_time.elapsed() < duration {
        for _ in 0..burst_size {
            event_stream_server.broadcast(SseData::random_deploy_accepted(&mut test_rng).0);
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
        for _ in 0..burst_size {
            event_stream_server.broadcast(SseData::random_deploy_processed(&mut test_rng));
        }
    }
}
