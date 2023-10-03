use core::time;
use derive_new::new;
use itertools::Itertools;
use serde_json::json;
use std::{
    fmt::{Display, Formatter},
    iter,
    ops::Div,
    thread,
    time::Duration,
};
use tempfile::TempDir;
use tokio::{
    sync::mpsc::{channel as mpsc_channel, Receiver, Sender},
    time::Instant,
};

use crate::{
    event_stream_server::{Config as EssConfig, EventStreamServer},
    utils::display_duration,
};
use casper_event_types::{sse_data::SseData, Filter as SseFilter};
use casper_types::{testing::TestRng, ProtocolVersion};
use warp::{path::end, Filter};

const TIME_BETWEEN_BLOCKS: Duration = Duration::from_secs(30);
const BLOCKS_IN_ERA: u64 = 4;
const NUMBER_OF_VALIDATORS: u16 = 100;
const NUMBER_OF_DEPLOYS_PER_BLOCK: u16 = 20;
const API_VERSION_1_4_0: ProtocolVersion = ProtocolVersion::from_parts(1, 4, 10);

type FrequencyOfStepEvents = u8;
type NumberOfDeployEventsInBurst = u64;

#[derive(Clone)]
pub enum Bound {
    Timed(Duration),
}

impl Display for Bound {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Bound::Timed(duration) => write!(f, "{}", display_duration(duration.to_owned())),
        }
    }
}

#[derive(Clone, new)]
pub struct GenericScenarioSettings {
    initial_phase: Bound,
    restart: Option<Restart>,
}

#[derive(Clone, new)]
pub struct Restart {
    delay_before_restart: Duration,
    final_phase: Bound,
}

#[derive(Clone)]
pub enum Scenario {
    Realistic(GenericScenarioSettings),
    LoadTestingStep(GenericScenarioSettings, FrequencyOfStepEvents),
    LoadTestingDeploy(GenericScenarioSettings, NumberOfDeployEventsInBurst),
}

impl Display for Scenario {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Scenario::Realistic(_) => write!(f, "Realistic"),
            Scenario::LoadTestingStep(_, _) => {
                write!(f, "Load Testing [Step]")
            }
            Scenario::LoadTestingDeploy(_, _) => {
                write!(f, "Load Testing [Deploy]")
            }
        }
    }
}

pub(crate) async fn spin_up_fake_event_stream(
    test_rng: TestRng,
    ess_config: EssConfig,
    scenario: Scenario,
) -> TestRng {
    let start = Instant::now();
    let (event_stream_server, log_details) = build_event_stream_server(ess_config, &scenario);
    let (events_sender, events_receiver) = mpsc_channel(500);
    let returned_test_rng = match scenario {
        Scenario::Realistic(settings) => {
            handle_realistic_scenario(
                test_rng,
                events_sender,
                events_receiver,
                event_stream_server,
                settings,
            )
            .await
        }
        Scenario::LoadTestingStep(settings, frequency) => {
            do_load_testing_step(
                test_rng,
                events_sender,
                events_receiver,
                event_stream_server,
                settings,
                frequency,
            )
            .await
        }
        Scenario::LoadTestingDeploy(settings, num_in_burst) => {
            do_load_testing_deploy(
                test_rng,
                events_sender,
                events_receiver,
                event_stream_server,
                settings,
                num_in_burst,
            )
            .await
        }
    };
    log_test_end(log_details, start);
    returned_test_rng
}

fn log_test_end(log_details: String, start: Instant) {
    println!(
        "{} :: Completed ({}s)",
        log_details,
        start.elapsed().as_secs()
    );
}

fn build_event_stream_server(
    ess_config: EssConfig,
    scenario: &Scenario,
) -> (EventStreamServer, String) {
    let cloned_address = ess_config.address.clone();
    let port = cloned_address.split(':').collect::<Vec<&str>>()[1];
    let log_details = format!("Fake Event Stream(:{}) :: Scenario: {}", port, scenario);
    println!("{} :: Started", log_details);
    let temp_dir = TempDir::new().expect("Error creating temporary directory");

    let event_stream_server = EventStreamServer::new(ess_config, temp_dir.path().to_path_buf())
        .expect("Error spinning up Event Stream Server");
    (event_stream_server, log_details)
}

async fn do_load_testing_deploy(
    mut test_rng: TestRng,
    events_sender: Sender<SseData>,
    mut events_receiver: Receiver<SseData>,
    mut event_stream_server: EventStreamServer,
    settings: GenericScenarioSettings,
    num_in_burst: u64,
) -> TestRng {
    let scenario_task = tokio::spawn(async move {
        load_testing_deploy(
            &mut test_rng,
            events_sender.clone(),
            settings.initial_phase,
            num_in_burst,
        )
        .await;

        if let Some(Restart {
            delay_before_restart,
            final_phase,
        }) = settings.restart
        {
            events_sender
                .send(SseData::Shutdown)
                .await
                .expect("Scenario::LoadTestingDeploy failed sending shutdown message!");

            tokio::time::sleep(delay_before_restart).await;
            load_testing_deploy(&mut test_rng, events_sender, final_phase, num_in_burst).await;
        }
        test_rng
    });

    let broadcasting_task = tokio::spawn(async move {
        while let Some(event) = events_receiver.recv().await {
            event_stream_server.broadcast(event, Some(SseFilter::Main), None);
        }
    });

    let (test_rng, _) = tokio::join!(scenario_task, broadcasting_task);
    test_rng.expect("Should have returned TestRng for re-use")
}

async fn do_load_testing_step(
    mut test_rng: TestRng,
    events_sender: Sender<SseData>,
    mut events_receiver: Receiver<SseData>,
    mut event_stream_server: EventStreamServer,
    settings: GenericScenarioSettings,
    frequency: u8,
) -> TestRng {
    let scenario_task = tokio::spawn(async move {
        load_testing_step(
            &mut test_rng,
            events_sender.clone(),
            settings.initial_phase,
            frequency,
        )
        .await;

        if let Some(Restart {
            delay_before_restart,
            final_phase,
        }) = settings.restart
        {
            events_sender
                .send(SseData::Shutdown)
                .await
                .expect("Scenario::LoadTestingStep failed sending SseData::Shutdown");

            tokio::time::sleep(delay_before_restart).await;
            load_testing_step(&mut test_rng, events_sender, final_phase, frequency).await;
        }
        test_rng
    });
    let broadcasting_task = tokio::spawn(async move {
        while let Some(event) = events_receiver.recv().await {
            event_stream_server.broadcast(event, Some(SseFilter::Main), None);
        }
    });
    let (test_rng, _) = tokio::join!(scenario_task, broadcasting_task);
    test_rng.expect("Should have returned TestRng for re-use")
}

async fn handle_realistic_scenario(
    mut test_rng: TestRng,
    events_sender: Sender<SseData>,
    mut events_receiver: Receiver<SseData>,
    mut event_stream_server: EventStreamServer,
    settings: GenericScenarioSettings,
) -> TestRng {
    let scenario_task = tokio::spawn(async move {
        realistic_event_streaming(&mut test_rng, events_sender.clone(), settings.initial_phase)
            .await;

        if let Some(Restart {
            delay_before_restart,
            final_phase,
        }) = settings.restart
        {
            events_sender
                .send(SseData::Shutdown)
                .await
                .expect("Scenario::Realistic failed sending SseData::Shutdown");

            tokio::time::sleep(delay_before_restart).await;
            realistic_event_streaming(&mut test_rng, events_sender, final_phase).await;
        }
        test_rng
    });
    let broadcasting_task = tokio::spawn(async move {
        while let Some(event) = events_receiver.recv().await {
            event_stream_server.broadcast(event, Some(SseFilter::Main), None);
        }
    });
    let (test_rng, _) = tokio::join!(scenario_task, broadcasting_task);
    test_rng.expect("Should have returned TestRng for re-use")
}

fn plus_twenty_percent(base_value: u64) -> u64 {
    let ten_percent = base_value / 10;
    base_value + 2 * ten_percent
}

async fn realistic_event_streaming(
    test_rng: &mut TestRng,
    events_sender: Sender<SseData>,
    bound: Bound,
) {
    let start = Instant::now();

    let loops_in_duration = match bound {
        Bound::Timed(duration) => {
            BLOCKS_IN_ERA * duration.div(TIME_BETWEEN_BLOCKS.as_secs() as u32).as_secs() + 2
        }
    };

    let test_data = prepare_data(test_rng, loops_in_duration);
    let interval = tokio::time::interval(TIME_BETWEEN_BLOCKS);
    events_sender
        .send(SseData::ApiVersion(API_VERSION_1_4_0))
        .await
        .unwrap();
    do_stream(interval, bound, start, events_sender, test_data).await;
}

type RealisticScenarioData = (
    Vec<SseData>,
    Vec<(SseData, casper_event_types::Deploy)>,
    Vec<SseData>,
    Vec<SseData>,
    Vec<SseData>,
    Vec<SseData>,
    Vec<SseData>,
);

fn prepare_data(test_rng: &mut TestRng, loops_in_duration: u64) -> RealisticScenarioData {
    let finality_signatures_per_loop = NUMBER_OF_VALIDATORS as u64;
    let total_finality_signature_events = finality_signatures_per_loop * loops_in_duration;
    let deploy_events_per_loop = NUMBER_OF_DEPLOYS_PER_BLOCK as u64;
    let total_deploy_events = deploy_events_per_loop * loops_in_duration;
    let total_block_added_events = loops_in_duration;
    let total_step_events = loops_in_duration / BLOCKS_IN_ERA;
    let block_added_events = iter::repeat_with(|| SseData::random_block_added(test_rng))
        .take(plus_twenty_percent(total_block_added_events) as usize)
        .collect_vec();
    let deploy_accepted_events = iter::repeat_with(|| SseData::random_deploy_accepted(test_rng))
        .take(plus_twenty_percent(total_deploy_events) as usize)
        .collect_vec();
    let deploy_expired_events = iter::repeat_with(|| SseData::random_deploy_expired(test_rng))
        .take((loops_in_duration / 2 + 1) as usize)
        .collect_vec();
    let deploy_processed_events = iter::repeat_with(|| SseData::random_deploy_processed(test_rng))
        .take(plus_twenty_percent(total_deploy_events) as usize)
        .collect_vec();
    let fault_events = iter::repeat_with(|| SseData::random_fault(test_rng))
        .take((loops_in_duration / 2 + 1) as usize)
        .collect_vec();
    let finality_signature_events =
        iter::repeat_with(|| SseData::random_finality_signature(test_rng))
            .take(plus_twenty_percent(total_finality_signature_events) as usize)
            .collect_vec();
    let step_events = iter::repeat_with(|| SseData::random_step(test_rng))
        .take(plus_twenty_percent(total_step_events) as usize)
        .collect_vec();
    (
        block_added_events,
        deploy_accepted_events,
        deploy_expired_events,
        deploy_processed_events,
        fault_events,
        finality_signature_events,
        step_events,
    )
}

#[allow(clippy::too_many_lines)]
async fn do_stream(
    mut interval: tokio::time::Interval,
    bound: Bound,
    start: Instant,
    events_sender: Sender<SseData>,
    data: RealisticScenarioData,
) {
    let (
        mut block_added_events,
        mut deploy_accepted_events,
        mut deploy_expired_events,
        mut deploy_processed_events,
        mut fault_events,
        mut finality_signature_events,
        mut step_events,
    ) = data;
    let mut era_counter: u16 = 0;
    'outer: loop {
        for _ in 0..BLOCKS_IN_ERA {
            interval.tick().await;
            match bound {
                Bound::Timed(duration) => {
                    if start.elapsed() >= duration {
                        break 'outer;
                    }
                }
            }
            emit_events(
                &events_sender,
                &mut finality_signature_events,
                &mut deploy_processed_events,
                &mut block_added_events,
                &mut deploy_accepted_events,
            )
            .await;
        }
        if era_counter % 2 == 0 {
            events_sender
                .send(deploy_expired_events.pop().unwrap())
                .await
                .expect("Failed sending deploy_expired_event");
        } else {
            events_sender
                .send(fault_events.pop().unwrap())
                .await
                .expect("Failed sending fault_event");
        }
        emit_step(&events_sender, &mut step_events).await;
        era_counter += 1;
    }
}

async fn emit_events(
    events_sender: &Sender<SseData>,
    finality_signature_events: &mut Vec<SseData>,
    deploy_processed_events: &mut Vec<SseData>,
    block_added_events: &mut Vec<SseData>,
    deploy_accepted_events: &mut Vec<(SseData, casper_event_types::Deploy)>,
) {
    emit_sig_events(events_sender, finality_signature_events).await;
    emit_deploy_processed_events(events_sender, deploy_processed_events).await;
    emit_block_added_events(events_sender, block_added_events).await;
    emit_deploy_accepted_events(events_sender, deploy_accepted_events).await;
}

async fn emit_block_added_events(
    events_sender: &Sender<SseData>,
    block_added_events: &mut Vec<SseData>,
) {
    events_sender
        .send(block_added_events.pop().unwrap())
        .await
        .expect("Failed sending block_added_event");
}

async fn emit_deploy_accepted_events(
    events_sender: &Sender<SseData>,
    deploy_accepted_events: &mut Vec<(SseData, casper_event_types::Deploy)>,
) {
    for _ in 0..NUMBER_OF_DEPLOYS_PER_BLOCK {
        events_sender
            .send(deploy_accepted_events.pop().unwrap().0)
            .await
            .expect("Failed sending deploy_accepted_event");
    }
}

async fn emit_step(events_sender: &Sender<SseData>, step_events: &mut Vec<SseData>) {
    events_sender
        .send(step_events.pop().unwrap())
        .await
        .expect("Failed sending step_event");
}

async fn emit_deploy_processed_events(
    events_sender: &Sender<SseData>,
    deploy_processed_events: &mut Vec<SseData>,
) {
    for _ in 0..NUMBER_OF_DEPLOYS_PER_BLOCK {
        events_sender
            .send(deploy_processed_events.pop().unwrap())
            .await
            .expect("Failed sending deploy_processed_events");
    }
}

async fn emit_sig_events(
    events_sender: &Sender<SseData>,
    finality_signature_events: &mut Vec<SseData>,
) {
    for _ in 0..NUMBER_OF_VALIDATORS {
        events_sender
            .send(finality_signature_events.pop().unwrap())
            .await
            .expect("Failed sending finality_signature_event");
    }
}

async fn load_testing_step(
    test_rng: &mut TestRng,
    event_sender: Sender<SseData>,
    bound: Bound,
    frequency: u8,
) {
    let start_time = Instant::now();
    event_sender
        .send(SseData::ApiVersion(API_VERSION_1_4_0))
        .await
        .unwrap();
    match bound {
        Bound::Timed(duration) => {
            while start_time.elapsed() < duration {
                event_sender
                    .send(SseData::random_step(test_rng))
                    .await
                    .expect("Bound::Timed Failed sending random_step");
                tokio::time::sleep(Duration::from_millis(1000 / frequency as u64)).await;
            }
        }
    }
}

async fn load_testing_deploy(
    test_rng: &mut TestRng,
    events_sender: Sender<SseData>,
    bound: Bound,
    burst_size: u64,
) {
    let start_time = Instant::now();

    events_sender
        .send(SseData::ApiVersion(API_VERSION_1_4_0))
        .await
        .unwrap();
    match bound {
        Bound::Timed(duration) => {
            while start_time.elapsed() < duration {
                for _ in 0..burst_size {
                    events_sender
                        .send(SseData::random_deploy_accepted(test_rng).0)
                        .await
                        .expect("failed sending random_deploy_accepted");
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
                for _ in 0..burst_size {
                    events_sender
                        .send(SseData::random_deploy_processed(test_rng))
                        .await
                        .expect("failed sending random_deploy_processed");
                }
            }
        }
    }
}

pub async fn setup_mock_build_version_server(port: u16) -> (Sender<()>, Receiver<()>) {
    setup_mock_build_version_server_with_version(port, "1.4.10".to_string()).await
}

pub async fn setup_mock_build_version_server_with_version(
    port: u16,
    version: String,
) -> (Sender<()>, Receiver<()>) {
    let (shutdown_tx, mut shutdown_rx) = mpsc_channel(10);
    let (after_shutdown_tx, after_shutdown_rx) = mpsc_channel(10);
    let api = warp::path!("status")
        .and(warp::get())
        .map(move || {
            let result = json!({ "build_version": version.clone() });
            warp::reply::json(&result)
        })
        .and(end());
    let server_thread = tokio::spawn(async move {
        let server = warp::serve(api)
            .bind_with_graceful_shutdown(([127, 0, 0, 1], port), async move {
                let _ = shutdown_rx.recv().await;
            })
            .1;
        server.await;
        let _ = after_shutdown_tx.send(()).await;
    });

    tokio::spawn(async move {
        let _ = server_thread.await;
    });
    wait_for_build_version_server_to_be_up(port).await;
    (shutdown_tx, after_shutdown_rx)
}

pub async fn wait_for_build_version_server_to_be_up(port: u16) {
    let max_attempts = 10;
    let mut attempts = 0;
    loop {
        attempts += 1;
        if attempts >= max_attempts {
            panic!(
                "Couldn't connect to status server in {} attempts",
                max_attempts
            );
        }
        let res = reqwest::get(format!("http://127.0.0.1:{}/status", port)).await;
        match res {
            Err(_) => {}
            Ok(response) => {
                if response.text().await.unwrap().contains("build_version") {
                    break;
                }
            }
        }
        thread::sleep(time::Duration::from_secs(1));
    }
}
