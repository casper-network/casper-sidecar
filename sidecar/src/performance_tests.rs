use std::{
    fmt::{Display, Formatter},
    time::Duration,
};

use derive_new::new;
use tabled::{object::Cell, Alignment, ModifyObject, Span, Style, TableIteratorExt, Tabled};
use tempfile::tempdir;
use tokio::{sync::mpsc::UnboundedReceiver, time::Instant};

use casper_event_listener::SseEvent;
use casper_types::AsymmetricType;

use super::*;
use crate::testing::{
    fake_event_stream::{spin_up_fake_event_stream, EventStreamScenario},
    testing_config::prepare_config,
};

const ACCEPTABLE_LATENCY: Duration = Duration::from_millis(1000);

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn check_latency_on_realistic_scenario() {
    performance_check(EventStreamScenario::Realistic, 120, ACCEPTABLE_LATENCY).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn check_latency_on_frequent_step_scenario() {
    performance_check(
        EventStreamScenario::LoadTestingStep(2),
        60,
        ACCEPTABLE_LATENCY,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn check_latency_on_fast_bursts_of_deploys_scenario() {
    performance_check(
        EventStreamScenario::LoadTestingDeploy(20),
        60,
        ACCEPTABLE_LATENCY,
    )
    .await;
}

#[derive(Clone, new)]
struct TimestampedEvent {
    event: SseData,
    timestamp: Instant,
}

#[derive(new)]
struct EventLatency {
    event: EventType,
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

    fn matches(&self, other: &Self) -> bool {
        match (&self.event, &other.event) {
            (SseData::ApiVersion(_), SseData::ApiVersion(_))
            | (SseData::BlockAdded { .. }, SseData::BlockAdded { .. })
            | (SseData::DeployAccepted { .. }, SseData::DeployAccepted { .. })
            | (SseData::DeployProcessed { .. }, SseData::DeployProcessed { .. })
            | (SseData::DeployExpired { .. }, SseData::DeployExpired { .. })
            | (SseData::Fault { .. }, SseData::Fault { .. })
            | (SseData::FinalitySignature(_), SseData::FinalitySignature(_))
            | (SseData::Step { .. }, SseData::Step { .. })
            | (SseData::Shutdown, SseData::Shutdown) => (),
            _ => return false,
        }
        self.identifier() == other.identifier()
    }
}

#[derive(new, Tabled)]
struct Results {
    #[tabled(rename = "Event Type")]
    event_type: EventType,
    #[tabled(rename = "Avg. Latency (ms)")]
    average_latency: u128,
    #[tabled(rename = "Total Received")]
    total_received: u16,
}

async fn performance_check(
    scenario: EventStreamScenario,
    duration_in_seconds: u64,
    acceptable_latency: Duration,
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

    let results = vec![
        Results::new(
            EventType::BlockAdded,
            average_latency_for_block_added.as_millis(),
            num_block_added_received,
        ),
        Results::new(
            EventType::DeployAccepted,
            average_latency_for_deploy_accepted.as_millis(),
            num_deploy_accepted_received,
        ),
        Results::new(
            EventType::DeployProcessed,
            average_latency_for_deploy_processed.as_millis(),
            num_deploy_processed_received,
        ),
        Results::new(
            EventType::DeployExpired,
            average_latency_for_deploy_expired.as_millis(),
            num_deploy_expired_received,
        ),
        Results::new(
            EventType::Step,
            average_latency_for_step.as_millis(),
            num_step_received,
        ),
        Results::new(
            EventType::Fault,
            average_latency_for_fault.as_millis(),
            num_fault_received,
        ),
        Results::new(
            EventType::FinalitySignature,
            average_latency_for_finality_signatures.as_millis(),
            num_finality_signatures_received,
        ),
        // This dummy entry is to create another row in the table - it is then overwritten by a span
        // containing the overall duration of the test.
        Results::new(EventType::ApiVersion, u128::default(), u16::default()),
    ];

    let total_rows = results.len();

    let horizontal_span = |row, col, span| {
        Cell(row, col)
            .modify()
            .with(Alignment::center())
            .with(Span::column(span))
    };
    let results_table = results
        .table()
        .with(
            horizontal_span(total_rows, 0, 3)
                .with(format!("\nTest Duration {}s", duration_in_seconds)),
        )
        .with(Style::rounded())
        .with(Style::correct_spans())
        .to_string();

    println!("{}", results_table);

    assert!(average_latency_for_block_added < acceptable_latency);
    assert!(average_latency_for_deploy_accepted < acceptable_latency);
    assert!(average_latency_for_deploy_expired < acceptable_latency);
    assert!(average_latency_for_deploy_processed < acceptable_latency);
    assert!(average_latency_for_fault < acceptable_latency);
    assert!(average_latency_for_finality_signatures < acceptable_latency);
    assert!(average_latency_for_step < acceptable_latency);
}

/// Returns the average latency in millis and the number of events averaged over
fn calculate_average_latency_for_type(
    event_type: EventType,
    combined_latencies: &mut Vec<EventLatency>,
) -> (Duration, u16) {
    let filtered_latencies = extract_latencies_by_type(event_type, combined_latencies);

    if filtered_latencies.is_empty() {
        return (Duration::from_millis(0), 0);
    }

    let average = filtered_latencies
        .iter()
        .map(|EventLatency { latency_millis, .. }| latency_millis)
        .sum::<u128>()
        .checked_div(filtered_latencies.len() as u128)
        .expect("Should have calculated average latency for BlockAdded events");

    (
        Duration::from_millis(average as u64),
        filtered_latencies.len() as u16,
    )
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
