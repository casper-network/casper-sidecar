use std::{
    fmt::{Display, Formatter},
    fs, iter,
    ops::Div,
    time::Duration,
};

use itertools::Itertools;
use tempfile::TempDir;
use tokio::sync::mpsc::unbounded_channel;
use tokio::time::Instant;

use casper_event_types::SseData;
use casper_types::{testing::TestRng, ProtocolVersion};

use crate::event_stream_server::{Config as EssConfig, EventStreamServer};

// Based on Mainnet cadence
const TIME_BETWEEN_BLOCKS: Duration = Duration::from_secs(30);
const NUMBER_OF_VALIDATORS: u16 = 100;
const NUMBER_OF_DEPLOYS_PER_BLOCK: u16 = 20;

type FrequencyOfStepEvents = u8;
type NumberOfDeployEventsInBurst = u8;
type NumberOfEventsToSend = u8;

pub enum EventStreamScenario {
    Counted(NumberOfEventsToSend),
    Realistic,
    LoadTestingStep(FrequencyOfStepEvents),
    LoadTestingDeploy(NumberOfDeployEventsInBurst),
}

impl Display for EventStreamScenario {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            EventStreamScenario::Counted(num) => write!(f, "Counted ({} events)", num),
            EventStreamScenario::Realistic => write!(f, "Realistic"),
            EventStreamScenario::LoadTestingStep(_) => {
                write!(f, "Load Testing [Step]")
            }
            EventStreamScenario::LoadTestingDeploy(_) => {
                write!(f, "Load Testing [Deploy]")
            }
        }
    }
}

pub async fn spin_up_fake_event_stream(
    test_rng: &'static mut TestRng,
    ess_config: EssConfig,
    scenario: EventStreamScenario,
    duration: Duration,
) {
    let start = Instant::now();

    let temp_dir = TempDir::new().expect("Error creating temporary directory");

    let cloned_address = ess_config.address.clone();
    let port = cloned_address.split(':').collect::<Vec<&str>>()[1];

    let event_stream_server = EventStreamServer::new(
        ess_config,
        temp_dir.path().to_path_buf(),
        ProtocolVersion::V1_0_0,
    )
    .expect("Error spinning up Event Stream Server");

    match scenario {
        EventStreamScenario::Counted(number_of_events) => {
            counted_event_streaming(test_rng, event_stream_server, number_of_events).await
        }
        EventStreamScenario::Realistic => {
            realistic_event_streaming(test_rng, event_stream_server, duration).await
        }
        EventStreamScenario::LoadTestingStep(frequency) => {
            repeat_step_events(test_rng, event_stream_server, duration, frequency).await
        }
        EventStreamScenario::LoadTestingDeploy(num_in_burst) => {
            fast_bursts_of_deploy_events(test_rng, event_stream_server, duration, num_in_burst)
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

fn plus_ten_percent(base_value: u64) -> u64 {
    let ten_percent = base_value / 10;
    base_value + ten_percent
}

async fn realistic_event_streaming(
    test_rng: &mut TestRng,
    mut server: EventStreamServer,
    duration: Duration,
) {
    let mut steps = Vec::new();

    for i in 6..=9 {
        let path_to_step_json = format!(
            "/home/george/casper/casperlabs/step_events/mainnet_step_700{}.json",
            i
        );
        let step_json_string = fs::read_to_string(path_to_step_json).unwrap();
        let big_step = serde_json::from_str::<SseData>(&step_json_string).unwrap();

        steps.push(big_step);
    }

    let (broadcast_sender, mut broadcast_receiver) = unbounded_channel();

    let cloned_sender = broadcast_sender.clone();

    let loops_in_duration = 4 * duration.div(TIME_BETWEEN_BLOCKS.as_secs() as u32).as_secs();
    let finality_signatures_per_loop = NUMBER_OF_VALIDATORS as u64;
    let total_finality_signature_events = finality_signatures_per_loop * loops_in_duration;
    let deploy_events_per_loop = NUMBER_OF_DEPLOYS_PER_BLOCK as u64;
    let total_deploy_events = deploy_events_per_loop * loops_in_duration;
    let total_block_added_events = loops_in_duration;

    let mut finality_signature_events =
        iter::repeat_with(|| SseData::random_finality_signature(test_rng))
            .take(plus_ten_percent(total_finality_signature_events) as usize)
            .collect_vec();
    let mut deploy_processed_events =
        iter::repeat_with(|| SseData::random_deploy_processed(test_rng))
            .take(plus_ten_percent(total_deploy_events) as usize)
            .collect_vec();
    let mut block_added_events = iter::repeat_with(|| SseData::random_block_added(test_rng))
        .take(plus_ten_percent(total_block_added_events) as usize)
        .collect_vec();
    let mut deploy_accepted_events =
        iter::repeat_with(|| SseData::random_deploy_accepted(test_rng))
            .take(plus_ten_percent(total_deploy_events) as usize)
            .collect_vec();
    // let mut step_events = iter::repeat_with(|| SseData::random_step(test_rng))
    //     .take(plus_ten_percent(total_step_events) as usize)
    //     .collect_vec();

    let frequent_handle = tokio::spawn(tokio::time::timeout(duration, async move {
        let mut interval = tokio::time::interval(TIME_BETWEEN_BLOCKS);

        loop {
            // Four Block cycles
            for _ in 0..4 {
                interval.tick().await;

                // Prior to each BlockAdded emit FinalitySignatures
                for _ in 0..NUMBER_OF_VALIDATORS {
                    let _ = cloned_sender.send(finality_signature_events.pop().unwrap());
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }

                // Emit DeployProcessed events for the next BlockAdded
                for _ in 0..NUMBER_OF_DEPLOYS_PER_BLOCK {
                    let _ = cloned_sender.send(deploy_processed_events.pop().unwrap());
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }

                // Emit the BlockAdded
                let _ = cloned_sender.send(block_added_events.pop().unwrap());

                // Emit DeployAccepted Events
                for _ in 0..NUMBER_OF_DEPLOYS_PER_BLOCK {
                    let _ = cloned_sender.send(deploy_accepted_events.pop().unwrap().0);
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
            // Then a Step
            let _ = cloned_sender.send(steps.pop().unwrap());
        }
    }));

    let cloned_sender = broadcast_sender.clone();

    let number_of_deploy_expired_events = 4;

    let mut deploy_expired_events = iter::repeat_with(|| SseData::random_deploy_expired(test_rng))
        .take(number_of_deploy_expired_events + 1)
        .collect_vec();

    let deploy_expired_events_handle = tokio::spawn(tokio::time::timeout(duration, async move {
        loop {
            tokio::time::sleep(duration / number_of_deploy_expired_events as u32).await;
            let _ = cloned_sender.send(deploy_expired_events.pop().unwrap());
        }
    }));

    let number_of_fault_events = 2;

    let mut fault_events = iter::repeat_with(|| SseData::random_fault(test_rng))
        .take(number_of_fault_events + 1)
        .collect_vec();

    let fault_events_handle = tokio::spawn(tokio::time::timeout(duration, async move {
        loop {
            tokio::time::sleep(duration / number_of_fault_events as u32).await;
            let _ = broadcast_sender.send(fault_events.pop().unwrap());
        }
    }));

    let broadcast_handle = tokio::spawn(tokio::time::timeout(duration, async move {
        while let Some(sse_data) = broadcast_receiver.recv().await {
            server.broadcast(sse_data);
        }
    }));

    let _ = tokio::join!(
        frequent_handle,
        deploy_expired_events_handle,
        fault_events_handle,
        broadcast_handle
    );
}

async fn repeat_step_events(
    test_rng: &mut TestRng,
    mut event_stream_server: EventStreamServer,
    duration: Duration,
    frequency: u8,
) {
    let start_time = Instant::now();

    while start_time.elapsed() < duration {
        event_stream_server.broadcast(SseData::random_step(test_rng));
        tokio::time::sleep(Duration::from_millis(1000 / frequency as u64)).await;
    }
}

async fn fast_bursts_of_deploy_events(
    test_rng: &mut TestRng,
    mut event_stream_server: EventStreamServer,
    duration: Duration,
    burst_size: u8,
) {
    let start_time = Instant::now();

    while start_time.elapsed() < duration {
        for _ in 0..burst_size {
            event_stream_server.broadcast(SseData::random_deploy_accepted(test_rng).0);
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
        for _ in 0..burst_size {
            event_stream_server.broadcast(SseData::random_deploy_processed(test_rng));
        }
    }
}

async fn counted_event_streaming(
    test_rng: &mut TestRng,
    mut event_stream_server: EventStreamServer,
    count: u8,
) {
    for _ in 0..count {
        event_stream_server.broadcast(SseData::random_block_added(test_rng));
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

#[tokio::test]
async fn check_step_flight_time() {
    let path_to_step_json =
        "/home/george/casper/casperlabs/step_events/mainnet_step_7006.json".to_string();
    let step_json_string = fs::read_to_string(path_to_step_json).unwrap();
    let big_step = serde_json::from_str::<SseData>(&step_json_string).unwrap();

    let (sender, mut receiver) = unbounded_channel();

    let sending_task_handle = tokio::spawn(async move {
        while let Some(_event) = receiver.recv().await {
            println!("Received event at: {}", chrono::Utc::now().time())
        }
    });

    let receiving_task_handle = tokio::spawn(async move {
        println!("Sending event at: {}", chrono::Utc::now().time());
        let _ = sender.send(big_step);
    });

    let _ = tokio::join!(receiving_task_handle, sending_task_handle);
}
