use super::*;
use crate::testing::fake_event_stream::{spin_up_fake_event_stream, EventStreamScenario};
use crate::testing::testing_config::prepare_config;
use casper_event_listener::SseEvent;
use casper_types::AsymmetricType;
use derive_new::new;
use std::fmt::{Display, Formatter};
use std::println;
use std::time::Duration;
use tabled::Tabled;
use tempfile::tempdir;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::time::Instant;

const ACCEPTABLE_LAG_IN_MILLIS: u128 = 1000;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn check_latency_on_realistic_scenario() {
    performance_check(
        EventStreamScenario::Realistic,
        120,
        ACCEPTABLE_LAG_IN_MILLIS,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn check_latency_on_frequent_step_scenario() {
    performance_check(
        EventStreamScenario::LoadTestingStep(2),
        60,
        ACCEPTABLE_LAG_IN_MILLIS,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn check_latency_on_fast_bursts_of_deploys_scenario() {
    performance_check(
        EventStreamScenario::LoadTestingDeploy(20),
        60,
        ACCEPTABLE_LAG_IN_MILLIS,
    )
    .await;
}

#[derive(Clone, new)]
struct TimestampedEvent {
    event: SseData,
    timestamp: Instant,
}

#[derive(Tabled, new)]
struct EventLatency {
    event: EventType,
    received: u16,
    latency_millis: u128,
}

#[derive(Clone, Debug, PartialEq)]
enum EventType {
    ApiVersion,
    BlockAdded,
    DeployAccepted,
    DeployExpired,
    DeployProcessed,
    Fault,
    FinalitySignature,
    Step,
    Shutdown,
}

impl Display for EventType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let string = match self {
            EventType::ApiVersion => "ApiVersion",
            EventType::BlockAdded => "BlockAdded",
            EventType::DeployAccepted => "DeployAccepted",
            EventType::DeployExpired => "DeployExpired",
            EventType::DeployProcessed => "DeployProcessed",
            EventType::Fault => "Fault",
            EventType::FinalitySignature => "FinalitySignature",
            EventType::Step => "Step",
            EventType::Shutdown => "Shutdown",
        };
        write!(f, "{}", string)
    }
}

impl TimestampedEvent {
    fn event_type(&self) -> EventType {
        match self.event {
            SseData::ApiVersion(_) => EventType::ApiVersion,
            SseData::BlockAdded { .. } => EventType::BlockAdded,
            SseData::DeployAccepted { .. } => EventType::DeployAccepted,
            SseData::DeployProcessed { .. } => EventType::DeployProcessed,
            SseData::DeployExpired { .. } => EventType::DeployExpired,
            SseData::Fault { .. } => EventType::Fault,
            SseData::FinalitySignature(_) => EventType::FinalitySignature,
            SseData::Step { .. } => EventType::Step,
            SseData::Shutdown => EventType::Shutdown,
        }
    }

    fn identifier(&self) -> String {
        match &self.event {
            SseData::ApiVersion(_) => "ApiVersion".to_string(),
            SseData::BlockAdded { block_hash, .. } => block_hash.to_string(),
            SseData::DeployAccepted { deploy } => deploy.id().to_string(),
            SseData::DeployProcessed { deploy_hash, .. } => deploy_hash.to_string(),
            SseData::DeployExpired { deploy_hash } => deploy_hash.to_string(),
            SseData::Fault {
                era_id, public_key, ..
            } => format!("{}-{}", era_id.value(), public_key.to_hex()),
            SseData::FinalitySignature(signature) => signature.signature.to_string(),
            SseData::Step { era_id, .. } => era_id.to_string(),
            SseData::Shutdown => "Shutdown".to_string(),
        }
    }

    fn time_since(&self, other: &Self) -> Duration {
        self.timestamp.duration_since(other.timestamp)
    }

    // Underscored _other as clippy was viewing it as unused
    fn matches(&self, _other: &Self) -> bool {
        matches!(self, _other) && self.identifier() == _other.identifier()
    }
}

async fn performance_check(
    scenario: EventStreamScenario,
    duration_in_seconds: u64,
    acceptable_latency_in_millis: u128,
) {
    let temp_storage_dir = tempdir().expect("Should have created a temporary storage directory");
    let testing_config = prepare_config(&temp_storage_dir);

    tokio::spawn(spin_up_fake_event_stream(
        testing_config.connection_port(),
        scenario,
        duration_in_seconds,
    ));

    tokio::spawn(run(testing_config.inner()));

    tokio::time::sleep(Duration::from_secs(1)).await;

    let source_url = format!("127.0.0.1:{}", testing_config.connection_port());
    let source_event_listener = EventListener::new(source_url, 0, 0, false).await.unwrap();
    let source_event_receiver = source_event_listener.consume_combine_streams().await;

    let sidecar_url = format!("127.0.0.1:{}", testing_config.event_stream_server_port());
    let sidecar_event_listener = EventListener::new(sidecar_url, 0, 0, false).await.unwrap();
    let sidecar_event_receiver = sidecar_event_listener.consume_combine_streams().await;

    let source_task_handle = tokio::spawn(push_timestamped_events_to_vecs(source_event_receiver));

    let sidecar_task_handle = tokio::spawn(push_timestamped_events_to_vecs(sidecar_event_receiver));

    let (source_task_result, sidecar_task_result) =
        tokio::join!(source_task_handle, sidecar_task_handle);

    let events_from_source = source_task_result.expect("Error recording events from source");
    let mut events_from_sidecar = sidecar_task_result.expect("Error recording events from sidecar");

    let mut event_latencies = Vec::new();

    for event_from_source in events_from_source {
        for i in 0..events_from_sidecar.len() {
            if event_from_source.matches(&events_from_sidecar[i]) {
                event_latencies.push(EventLatency::new(
                    event_from_source.event_type(),
                    0,
                    events_from_sidecar[i]
                        .time_since(&event_from_source)
                        .as_millis(),
                ));
                events_from_sidecar.remove(i);
                break;
            }
        }
    }

    assert!(!event_latencies.is_empty());

    let (average_latency_for_finality_signatures, num_finality_signatures_received) =
        calculate_average_latency_for_type(EventType::FinalitySignature, &mut event_latencies);
    let (average_latency_for_deploy_processed, num_deploy_processed_received) =
        calculate_average_latency_for_type(EventType::DeployProcessed, &mut event_latencies);
    let (average_latency_for_deploy_accepted, num_deploy_accepted_received) =
        calculate_average_latency_for_type(EventType::DeployAccepted, &mut event_latencies);
    let (average_latency_for_block_added, num_block_added_received) =
        calculate_average_latency_for_type(EventType::BlockAdded, &mut event_latencies);
    let (average_latency_for_step, num_step_received) =
        calculate_average_latency_for_type(EventType::Step, &mut event_latencies);
    let (average_latency_for_deploy_expired, num_deploy_expired_received) =
        calculate_average_latency_for_type(EventType::DeployExpired, &mut event_latencies);
    let (average_latency_for_fault, num_fault_received) =
        calculate_average_latency_for_type(EventType::Fault, &mut event_latencies);

    let average_latencies = vec![
        EventLatency::new(
            EventType::BlockAdded,
            num_block_added_received,
            average_latency_for_block_added,
        ),
        EventLatency::new(
            EventType::DeployAccepted,
            num_deploy_accepted_received,
            average_latency_for_deploy_accepted,
        ),
        EventLatency::new(
            EventType::DeployProcessed,
            num_deploy_processed_received,
            average_latency_for_deploy_processed,
        ),
        EventLatency::new(
            EventType::DeployExpired,
            num_deploy_expired_received,
            average_latency_for_deploy_expired,
        ),
        EventLatency::new(EventType::Step, num_step_received, average_latency_for_step),
        EventLatency::new(
            EventType::Fault,
            num_fault_received,
            average_latency_for_fault,
        ),
        EventLatency::new(
            EventType::FinalitySignature,
            num_finality_signatures_received,
            average_latency_for_finality_signatures,
        ),
    ];

    println!("{}", tabled::Table::new(average_latencies));

    assert!(average_latency_for_block_added < acceptable_latency_in_millis);
    assert!(average_latency_for_deploy_accepted < acceptable_latency_in_millis);
    assert!(average_latency_for_deploy_expired < acceptable_latency_in_millis);
    assert!(average_latency_for_deploy_processed < acceptable_latency_in_millis);
    assert!(average_latency_for_fault < acceptable_latency_in_millis);
    assert!(average_latency_for_finality_signatures < acceptable_latency_in_millis);
    assert!(average_latency_for_step < acceptable_latency_in_millis);
}

/// Returns the average latency in millis and the number of events averaged over
fn calculate_average_latency_for_type(
    event_type: EventType,
    combined_latencies: &mut Vec<EventLatency>,
) -> (u128, u16) {
    let filtered_latencies = extract_latencies_by_type(event_type, combined_latencies);

    if filtered_latencies.is_empty() {
        return (0, 0);
    }

    let average = filtered_latencies
        .iter()
        .map(|EventLatency { latency_millis, .. }| latency_millis)
        .sum::<u128>()
        .checked_div(filtered_latencies.len() as u128)
        .expect("Should have calculated average latency for BlockAdded events");

    (average, filtered_latencies.len() as u16)
}

fn extract_latencies_by_type(
    event_type: EventType,
    combined_latencies: &mut Vec<EventLatency>,
) -> Vec<EventLatency> {
    let mut filtered_latencies = Vec::new();
    let mut i = 0;
    while i < combined_latencies.len() {
        if combined_latencies[i].event == event_type {
            let latency = combined_latencies.remove(i);
            filtered_latencies.push(latency);
        } else {
            i += 1;
        }
    }

    filtered_latencies
}

async fn push_timestamped_events_to_vecs(
    mut event_stream: UnboundedReceiver<SseEvent>,
) -> Vec<TimestampedEvent> {
    let mut events_vec: Vec<TimestampedEvent> = Vec::new();

    while let Some(event) = event_stream.recv().await {
        let received_timestamp = Instant::now();
        events_vec.push(TimestampedEvent::new(event.data, received_timestamp));
    }

    events_vec
}
