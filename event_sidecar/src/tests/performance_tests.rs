use crate::{
    database::postgresql_database::PostgreSqlDatabase,
    testing::fake_event_stream::Bound,
    tests::integration_tests::fetch_data_from_endpoint,
    types::database::DatabaseReader,
    utils::tests::{
        build_postgres_based_test_config, unpack_test_config_and_run, wait_for_n_messages,
    },
};
use crate::{
    event_stream_server::Config as EssConfig,
    testing::{
        fake_event_stream::{
            setup_mock_build_version_server, spin_up_fake_event_stream, GenericScenarioSettings,
            Scenario,
        },
        testing_config::prepare_config,
    },
    utils::tests::display_duration,
};
use casper_event_listener::{EventListenerBuilder, NodeConnectionInterface, SseEvent};
use casper_event_types::sse_data::SseData;
use casper_types::{testing::TestRng, AsymmetricType};
use colored::Colorize;
use derive_new::new;
use std::{
    collections::HashMap,
    fmt::{Display, Formatter},
    net::IpAddr,
    str::FromStr,
    time::Duration,
};
use tabled::{object::Cell, Alignment, ModifyObject, Span, Style, TableIteratorExt, Tabled};
use tempfile::tempdir;
use tokio::{
    sync::mpsc::{self, Receiver},
    task::JoinHandle,
    time::{sleep, Instant},
};
use tokio_util::sync::CancellationToken;

const ACCEPTABLE_LATENCY: Duration = Duration::from_millis(1000);

#[tokio::test(flavor = "multi_thread", worker_threads = 5)]
#[ignore]
async fn check_latency_on_realistic_scenario() {
    let duration = Duration::from_secs(120);
    performance_check(
        Scenario::Realistic(GenericScenarioSettings::new(Bound::Timed(duration), None)),
        duration,
        ACCEPTABLE_LATENCY,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn check_latency_on_load_testing_step_scenario() {
    let duration = Duration::from_secs(60);

    performance_check(
        Scenario::LoadTestingStep(
            GenericScenarioSettings::new(Bound::Timed(duration), None),
            2,
        ),
        duration,
        ACCEPTABLE_LATENCY,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn check_latency_on_load_testing_deploys_scenario() {
    let duration = Duration::from_secs(60);

    performance_check(
        Scenario::LoadTestingDeploy(
            GenericScenarioSettings::new(Bound::Timed(duration), None),
            20,
        ),
        duration,
        ACCEPTABLE_LATENCY,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 5)]
#[ignore]
async fn test_event_throughput() {
    let duration_in_secs = 40;
    let (
        testing_config,
        _temp_storage_dir,
        node_port_for_sse_connection,
        node_port_for_rest_connection,
        event_stream_server_port,
        test_context,
    ) = build_postgres_based_test_config(5, 3).await;
    let duration = Duration::from_secs(duration_in_secs);
    let scenario = Scenario::Spam(Bound::Timed(duration));
    let cancellation_token = CancellationToken::new();

    tokio::spawn(fake_event_stream(
        scenario,
        node_port_for_sse_connection,
        node_port_for_rest_connection,
    ));
    tokio::spawn(async move { unpack_test_config_and_run(testing_config, false).await });

    let handle =
        start_counting_outbound_events(cancellation_token.clone(), event_stream_server_port).await;
    //Let the fake event stream push events for some time
    sleep(duration).await;
    cancellation_token.cancel();

    let (out_evts_per_sec, db_evts_per_sec) =
        calculate_events_per_sec(handle, &test_context.db, duration_in_secs).await;
    println!("out_evts_per_sec: {out_evts_per_sec}. db_evts_per_sec: {db_evts_per_sec}");
    assert!(out_evts_per_sec > 400.0);
    assert!(db_evts_per_sec > 400.0);
}

// This can be uncommented to use as a live test against a node in a real-world network i.e. Mainnet.
// The ip address and port would need to be changed to point to the desired node.
// #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
// async fn check_latency_against_live_node() {
//     live_performance_check(
//         "127.0.0.1".to_string(),
//         18101,
//         Duration::from_secs(60 * 10),
//         ACCEPTABLE_LATENCY,
//     )
//     .await;
// }

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
pub(crate) enum EventType {
    ApiVersion,
    SidecarVersion,
    BlockAdded,
    DeployAccepted,
    DeployExpired,
    DeployProcessed,
    Fault,
    FinalitySignature,
    Step,
    Shutdown,
}

impl From<SseData> for EventType {
    fn from(sse_data: SseData) -> Self {
        match sse_data {
            SseData::ApiVersion(_) => EventType::ApiVersion,
            SseData::SidecarVersion(_) => EventType::SidecarVersion,
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
}

impl Display for EventType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let string = match self {
            EventType::ApiVersion => "ApiVersion",
            EventType::SidecarVersion => "SidecarVersion",
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
        self.event.clone().into()
    }

    fn identifier(&self) -> String {
        match &self.event {
            SseData::ApiVersion(_) => "ApiVersion".to_string(),
            SseData::SidecarVersion(_) => "SidecarVersion".to_string(),
            SseData::BlockAdded { block_hash, .. } => block_hash.to_string(),
            SseData::DeployAccepted { deploy } => deploy.hash().to_string(),
            SseData::DeployProcessed { deploy_hash, .. } => deploy_hash.to_string(),
            SseData::DeployExpired { deploy_hash } => deploy_hash.to_string(),
            SseData::Fault {
                era_id, public_key, ..
            } => format!("{}-{}", era_id.value(), public_key.to_hex()),
            SseData::FinalitySignature(signature) => signature.signature().to_string(),
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
    #[tabled(rename = "Avg. Latency (ms)", display_with = "highlight_slow_latency")]
    average_latency: u128,
    #[tabled(rename = "Total Received")]
    total_received: u16,
}

fn highlight_slow_latency(latency: &u128) -> String {
    let millis = latency.to_owned();
    if millis < ACCEPTABLE_LATENCY.as_millis() {
        millis.to_string()
    } else {
        millis.to_string().red().to_string()
    }
}

#[allow(clippy::too_many_lines)]
async fn performance_check(scenario: Scenario, duration: Duration, acceptable_latency: Duration) {
    let test_rng = TestRng::new();

    let temp_storage_dir = tempdir().expect("Should have created a temporary storage directory");
    let mut testing_config = prepare_config(&temp_storage_dir);
    testing_config.add_connection(None, None, None);
    let node_port_for_sse_connection = testing_config
        .event_server_config
        .connections
        .first()
        .unwrap()
        .sse_port;
    let node_port_for_rest_connection = testing_config
        .event_server_config
        .connections
        .first()
        .unwrap()
        .rest_port;
    let (_shutdown_tx, _after_shutdown_rx) =
        setup_mock_build_version_server(node_port_for_rest_connection).await;

    let ess_config = EssConfig::new(node_port_for_sse_connection, None, None);

    tokio::spawn(spin_up_fake_event_stream(test_rng, ess_config, scenario));

    tokio::spawn(async move { unpack_test_config_and_run(testing_config, false).await });

    tokio::time::sleep(Duration::from_secs(1)).await;
    let ip_address = IpAddr::from_str("127.0.0.1").expect("Couldn't parse IpAddr");
    let node_interface = NodeConnectionInterface {
        ip_address,
        sse_port: node_port_for_sse_connection,
        rest_port: node_port_for_rest_connection,
    };
    let (node_event_tx, node_event_rx) = mpsc::channel(100);
    let mut node_event_listener = EventListenerBuilder {
        node: node_interface,
        max_connection_attempts: 5,
        delay_between_attempts: Duration::from_secs(1),
        allow_partial_connection: false,
        sse_event_sender: node_event_tx,
        connection_timeout: Duration::from_secs(100),
        sleep_between_keep_alive_checks: Duration::from_secs(100),
        no_message_timeout: Duration::from_secs(100),
    }
    .build()
    .unwrap();

    tokio::spawn(async move {
        let res = node_event_listener.stream_aggregated_events().await;
        if let Err(error) = res {
            println!("Node listener Error: {}", error)
        }
    });

    let (sidecar_event_tx, sidecar_event_rx) = mpsc::channel(100);

    let sidecar_node_interface = NodeConnectionInterface {
        ip_address: IpAddr::from_str("127.0.0.1").expect("Couldn't parse IpAddr"),
        sse_port: node_port_for_sse_connection,
        rest_port: node_port_for_rest_connection,
    };
    let mut sidecar_event_listener = EventListenerBuilder {
        node: sidecar_node_interface,
        max_connection_attempts: 5,
        delay_between_attempts: Duration::from_secs(1),
        allow_partial_connection: false,
        sse_event_sender: sidecar_event_tx,
        connection_timeout: Duration::from_secs(100),
        sleep_between_keep_alive_checks: Duration::from_secs(100),
        no_message_timeout: Duration::from_secs(100),
    }
    .build()
    .unwrap();
    tokio::spawn(async move {
        let res = sidecar_event_listener.stream_aggregated_events().await;
        if let Err(error) = res {
            println!("Sidecar listener Error: {}", error)
        }
    });

    let node_task_handle = tokio::spawn(push_timestamped_events_to_vecs(node_event_rx, None));

    let sidecar_task_handle = tokio::spawn(push_timestamped_events_to_vecs(sidecar_event_rx, None));

    let (node_task_result, sidecar_task_result) =
        tokio::join!(node_task_handle, sidecar_task_handle);
    let node_events = node_task_result.expect("Error recording events from node");
    let sidecar_events = sidecar_task_result.expect("Error recording events from sidecar");
    let event_latencies = compare_events_collect_latencies(node_events, sidecar_events);

    assert!(
        !event_latencies.is_empty(),
        "Should have compiled a list of event latencies - events may not have been received at all"
    );

    let event_types_ordered_for_efficiency = vec![
        EventType::FinalitySignature,
        EventType::DeployAccepted,
        EventType::DeployProcessed,
        EventType::BlockAdded,
        EventType::Step,
        EventType::DeployExpired,
        EventType::Fault,
    ];

    let (average_latencies, number_of_events_received) =
        calculate_average_latencies(event_latencies, event_types_ordered_for_efficiency);

    let results = create_results_from_data(&average_latencies, number_of_events_received);

    let results_table = build_table_from_results(results, duration);

    println!("{}", results_table);

    check_latencies_are_acceptable(average_latencies, acceptable_latency);
}

// This is only used by the `check_latency_against_live_node` test which is generally commented out.
/*#[allow(unused)]
async fn live_performance_check(
    ip_address: String,
    port: u16,
    duration: Duration,
    acceptable_latency: Duration,
) {
    let temp_storage_dir = tempdir().expect("Should have created a temporary storage directory");
    let mut testing_config = prepare_config(&temp_storage_dir);
    let port_for_connection = testing_config.add_connection(Some(ip_address.clone()), Some(port));

    tokio::spawn(run(testing_config.inner()));

    tokio::time::sleep(Duration::from_secs(1)).await;

    let source_url = format!("{}:{}", ip_address, port_for_connection);
    let source_event_listener = EventListener::new(source_url, 0, 0, false).await.unwrap();
    let source_event_receiver = source_event_listener
        .consume_combine_streams()
        .await
        .unwrap();

    let sidecar_url = format!("127.0.0.1:{}", testing_config.event_stream_server_port());
    let sidecar_event_listener = EventListener::new(sidecar_url, 0, 0, false).await.unwrap();
    let sidecar_event_receiver = sidecar_event_listener
        .consume_combine_streams()
        .await
        .unwrap();

    let source_task_handle = tokio::spawn(push_timestamped_events_to_vecs(
        source_event_receiver,
        Some(duration),
    ));

    let sidecar_task_handle = tokio::spawn(push_timestamped_events_to_vecs(
        sidecar_event_receiver,
        Some(duration),
    ));

    let (source_task_result, sidecar_task_result) =
        tokio::join!(source_task_handle, sidecar_task_handle);

    let events_from_source = source_task_result.expect("Error recording events from source");
    let events_from_sidecar = sidecar_task_result.expect("Error recording events from sidecar");

    let event_latencies = compare_events_collect_latencies(events_from_source, events_from_sidecar);

    assert!(
        !event_latencies.is_empty(),
        "Should have compiled a list of event latencies - events may not have been received at all"
    );

    let event_types_ordered_for_efficiency = vec![
        EventType::FinalitySignature,
        EventType::DeployAccepted,
        EventType::DeployProcessed,
        EventType::BlockAdded,
        EventType::Step,
        EventType::DeployExpired,
        EventType::Fault,
    ];

    let (average_latencies, number_of_events_received) =
        calculate_average_latencies(event_latencies, event_types_ordered_for_efficiency);

    let results = create_results_from_data(&average_latencies, number_of_events_received);

    let results_table = build_table_from_results(results, duration);

    println!("{}", results_table);

    check_latencies_are_acceptable(average_latencies, acceptable_latency);
}*/

fn check_latencies_are_acceptable(
    average_latencies: HashMap<String, Duration>,
    acceptable_latency: Duration,
) {
    let event_types = vec![
        EventType::BlockAdded,
        EventType::DeployAccepted,
        EventType::DeployExpired,
        EventType::DeployProcessed,
        EventType::Fault,
        EventType::FinalitySignature,
        EventType::Step,
    ];

    for event_type in event_types {
        let key = event_type.to_string();
        let average_latency_for_type = average_latencies
            .get(&key)
            .unwrap_or_else(|| panic!("Should have retrieved average latency for {}", event_type));

        assert!(
            average_latency_for_type < &acceptable_latency,
            "Latency of {} events exceeded acceptable value. Acceptable value {} [s]",
            event_type,
            acceptable_latency.as_secs_f64(),
        )
    }
}

fn create_results_from_data(
    average_latencies: &HashMap<String, Duration>,
    num_events_received: HashMap<String, u16>,
) -> Vec<Results> {
    let event_types_ordered_for_display = vec![
        EventType::BlockAdded,
        EventType::DeployAccepted,
        EventType::DeployExpired,
        EventType::DeployProcessed,
        EventType::Fault,
        EventType::FinalitySignature,
        EventType::Step,
    ];

    let mut results = Vec::new();

    for event_type in event_types_ordered_for_display {
        let key = event_type.to_string();
        let average_latency_for_type = average_latencies
            .get(&key)
            .unwrap_or_else(|| panic!("Should have retrieved average latency for {}", event_type));
        let number_of_events_received_for_type =
            num_events_received.get(&key).unwrap_or_else(|| {
                panic!(
                    "Should have retrieved number of events received for {}",
                    event_type
                )
            });

        results.push(Results::new(
            event_type,
            average_latency_for_type.as_millis(),
            number_of_events_received_for_type.to_owned(),
        ));
    }

    // This dummy entry is to create another row at the bottom of the table - it will be overwritten by a span
    // containing the overall duration of the test.
    results.push(Results::new(
        EventType::ApiVersion,
        u128::default(),
        u16::default(),
    ));

    results
}

fn calculate_average_latencies(
    mut event_latencies: Vec<EventLatency>,
    type_order: Vec<EventType>,
) -> (HashMap<String, Duration>, HashMap<String, u16>) {
    let mut average_latencies = HashMap::new();
    let mut number_of_events_received = HashMap::new();

    for event_type in type_order {
        let (average_latency_for_type, num_event_type_received) =
            calculate_average_latency_for_type(&event_type, &mut event_latencies);
        average_latencies.insert(event_type.clone().to_string(), average_latency_for_type);
        number_of_events_received.insert(event_type.to_string(), num_event_type_received);
    }

    (average_latencies, number_of_events_received)
}

fn build_table_from_results(results: Vec<Results>, duration: Duration) -> String {
    let total_rows = results.len();

    let horizontal_span = |row, col, span| {
        Cell(row, col)
            .modify()
            .with(Alignment::center())
            .with(Span::column(span))
    };

    results
        .table()
        .with(
            horizontal_span(total_rows, 0, 3)
                .with(format!("\nTest Duration {}", display_duration(duration))),
        )
        .with(Style::rounded())
        .with(Style::correct_spans())
        .to_string()
}

fn compare_events_collect_latencies(
    events_from_source: Vec<TimestampedEvent>,
    mut events_from_sidecar: Vec<TimestampedEvent>,
) -> Vec<EventLatency> {
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

    event_latencies
}

/// Returns the average latency in millis and the number of events averaged over
fn calculate_average_latency_for_type(
    event_type: &EventType,
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
    event_type: &EventType,
    combined_latencies: &mut Vec<EventLatency>,
) -> Vec<EventLatency> {
    let mut filtered_latencies = Vec::new();
    let mut i = 0;
    while i < combined_latencies.len() {
        if combined_latencies[i].event == *event_type {
            let latency = combined_latencies.remove(i);
            filtered_latencies.push(latency);
        } else {
            i += 1;
        }
    }

    filtered_latencies
}

/// `timeout` should be set for live tests where the source is not expected to shutdown, because the receiver won't run out of events and therefore will never exit.
async fn push_timestamped_events_to_vecs(
    mut event_stream: Receiver<SseEvent>,
    duration: Option<Duration>,
) -> Vec<TimestampedEvent> {
    let mut events_vec: Vec<TimestampedEvent> = Vec::new();

    if let Some(duration) = duration {
        let start = Instant::now();

        while let Some(event) = event_stream.recv().await {
            if start.elapsed() > duration {
                break;
            }

            let received_timestamp = Instant::now();
            events_vec.push(TimestampedEvent::new(event.data, received_timestamp));
        }
    } else {
        while let Some(event) = event_stream.recv().await {
            let received_timestamp = Instant::now();
            events_vec.push(TimestampedEvent::new(event.data, received_timestamp));
        }
    }

    events_vec
}

async fn fake_event_stream(
    scenario: Scenario,
    node_port_for_sse_connection: u16,
    node_port_for_rest_connection: u16,
) {
    let test_rng = TestRng::new();
    let (_shutdown_tx, _after_shutdown_rx) =
        setup_mock_build_version_server(node_port_for_rest_connection).await;
    let ess_config = EssConfig::new(node_port_for_sse_connection, None, None);
    spin_up_fake_event_stream(test_rng, ess_config, scenario).await;
}

async fn start_counting_outbound_events(
    cancellation_token: CancellationToken,
    event_stream_server_port: u16,
) -> JoinHandle<u32> {
    let (_, receiver) =
        fetch_data_from_endpoint("/events/deploys?start_from=0", event_stream_server_port).await;
    let mut receiver = wait_for_n_messages(1, receiver, Duration::from_secs(120)).await;
    tokio::spawn(async move {
        let mut counter = 0;
        while !cancellation_token.is_cancelled() {
            if receiver.recv().await.is_some() {
                counter += 1;
            } else {
                break;
            }
        }
        counter
    })
}

async fn calculate_events_per_sec(
    handle: JoinHandle<u32>,
    db: &PostgreSqlDatabase,
    duration_in_secs: u64,
) -> (f32, f32) {
    let outbound_sse_events = handle.await.unwrap();
    let count = db.get_number_of_events().await.unwrap();
    let events_per_sec = count as f32 / duration_in_secs as f32;
    let outbound_sse_events_per_sec = outbound_sse_events as f32 / duration_in_secs as f32;
    (events_per_sec, outbound_sse_events_per_sec)
}
