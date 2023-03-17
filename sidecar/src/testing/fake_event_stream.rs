use derive_new::new;
use itertools::Itertools;
use serde_json::json;
use std::{
    fmt::{Display, Formatter},
    iter,
    ops::{Deref, Div},
    sync::{Arc, Mutex},
    time::Duration,
};
use tempfile::TempDir;
use tokio::{
    sync::mpsc::{channel as mpsc_channel, Sender},
    time::Instant,
};

use casper_event_types::sse_data::SseData;
use casper_types::{testing::TestRng, ProtocolVersion};

use crate::{
    event_stream_server::{Config as EssConfig, EventStreamServer},
    utils::display_duration,
};
use warp::Filter;

const TIME_BETWEEN_BLOCKS: Duration = Duration::from_secs(30);
const BLOCKS_IN_ERA: u64 = 4;
const NUMBER_OF_VALIDATORS: u16 = 100;
const NUMBER_OF_DEPLOYS_PER_BLOCK: u16 = 20;
const API_VERSION_1_4_0: ProtocolVersion = ProtocolVersion::from_parts(1, 4, 10);

type FrequencyOfStepEvents = u8;
type NumberOfDeployEventsInBurst = u64;
type NumberOfEventsToSend = u64;

#[derive(Clone)]
pub enum Bound {
    Timed(Duration),
    Counted(NumberOfEventsToSend),
}

impl Display for Bound {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Bound::Timed(duration) => write!(f, "{}", display_duration(duration.to_owned())),
            Bound::Counted(count) => write!(f, "{}", count),
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
    Counted(GenericScenarioSettings),
    Realistic(GenericScenarioSettings),
    LoadTestingStep(GenericScenarioSettings, FrequencyOfStepEvents),
    LoadTestingDeploy(GenericScenarioSettings, NumberOfDeployEventsInBurst),
}

impl Display for Scenario {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Scenario::Counted(GenericScenarioSettings { initial_phase, .. }) => {
                write!(f, "Counted ({})", initial_phase)
            }
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
    test_rng: &'static mut TestRng,
    ess_config: EssConfig,
    scenario: Scenario,
) -> &'static mut TestRng {
    let cloned_address = ess_config.address.clone();
    let port = cloned_address.split(':').collect::<Vec<&str>>()[1];
    let log_details = format!("Fake Event Stream(:{}) :: Scenario: {}", port, scenario);
    println!("{} :: Started", log_details);

    let start = Instant::now();

    let temp_dir = TempDir::new().expect("Error creating temporary directory");

    let mut event_stream_server =
        EventStreamServer::new(ess_config.clone(), temp_dir.path().to_path_buf())
            .expect("Error spinning up Event Stream Server");

    let (events_sender, mut events_receiver) = mpsc_channel(500);

    let returned_test_rng = match scenario {
        Scenario::Counted(settings) => {
            let scenario_task = tokio::spawn(async move {
                counted_event_streaming(test_rng, events_sender.clone(), settings.initial_phase)
                    .await;

                if let Some(Restart {
                    delay_before_restart,
                    final_phase,
                }) = settings.restart
                {
                    events_sender
                        .send(SseData::Shutdown)
                        .await
                        .expect("Scenario::Counted failed sending shutdown message!");

                    tokio::time::sleep(delay_before_restart).await;
                    counted_event_streaming(test_rng, events_sender, final_phase).await;
                }
                test_rng
            });

            let broadcasting_task = tokio::spawn(async move {
                while let Some(event) = events_receiver.recv().await {
                    event_stream_server.broadcast(event, None);
                }
            });

            let (test_rng, _) = tokio::join!(scenario_task, broadcasting_task);
            test_rng.expect("Should have returned TestRng for re-use")
        }
        Scenario::Realistic(settings) => {
            let scenario_task = tokio::spawn(async move {
                realistic_event_streaming(test_rng, events_sender.clone(), settings.initial_phase)
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
                    realistic_event_streaming(test_rng, events_sender, final_phase).await;
                }
                test_rng
            });

            let broadcasting_task = tokio::spawn(async move {
                while let Some(event) = events_receiver.recv().await {
                    event_stream_server.broadcast(event, None);
                }
            });

            let (test_rng, _) = tokio::join!(scenario_task, broadcasting_task);
            test_rng.expect("Should have returned TestRng for re-use")
        }
        Scenario::LoadTestingStep(settings, frequency) => {
            let scenario_task = tokio::spawn(async move {
                load_testing_step(
                    test_rng,
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
                    load_testing_step(test_rng, events_sender, final_phase, frequency).await;
                }
                test_rng
            });

            let broadcasting_task = tokio::spawn(async move {
                while let Some(event) = events_receiver.recv().await {
                    event_stream_server.broadcast(event, None);
                }
            });

            let (test_rng, _) = tokio::join!(scenario_task, broadcasting_task);
            test_rng.expect("Should have returned TestRng for re-use")
        }
        Scenario::LoadTestingDeploy(settings, num_in_burst) => {
            let scenario_task = tokio::spawn(async move {
                load_testing_deploy(
                    test_rng,
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
                    load_testing_deploy(test_rng, events_sender, final_phase, num_in_burst).await;
                }
                test_rng
            });

            let broadcasting_task = tokio::spawn(async move {
                while let Some(event) = events_receiver.recv().await {
                    event_stream_server.broadcast(event, None);
                }
            });

            let (test_rng, _) = tokio::join!(scenario_task, broadcasting_task);
            test_rng.expect("Should have returned TestRng for re-use")
        }
    };

    println!(
        "{} :: Completed ({}s)",
        log_details,
        start.elapsed().as_secs()
    );
    returned_test_rng
}

fn plus_twenty_percent(base_value: u64) -> u64 {
    let ten_percent = base_value / 10;
    base_value + 2 * ten_percent
}

async fn counted_event_streaming(
    test_rng: &mut TestRng,
    event_sender: Sender<SseData>,
    count: Bound,
) {
    if let Bound::Counted(count) = count {
        event_sender
            .send(SseData::ApiVersion(API_VERSION_1_4_0))
            .await
            .unwrap();
        let mut events_sent = 0;

        while events_sent <= count {
            event_sender
                .send(SseData::random_deploy_accepted(test_rng).0)
                .await
                .expect("counted_event_streaming failed sending random_deploy_accepted!");
            event_sender
                .send(SseData::random_block_added(test_rng))
                .await
                .expect("counted_event_streaming failed sending random_block_added!");
            event_sender
                .send(SseData::random_finality_signature(test_rng))
                .await
                .expect("counted_event_streaming failed sending random_finality_signature!");
            events_sent += 3;
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    } else {
        panic!("Should have used Bound::Counted for counted_event_streaming")
    }
}

async fn realistic_event_streaming(
    test_rng: &mut TestRng,
    events_sender: Sender<SseData>,
    bound: Bound,
) {
    let start = Instant::now();

    let events_per_loop =
        BLOCKS_IN_ERA * (NUMBER_OF_VALIDATORS + 2 * NUMBER_OF_DEPLOYS_PER_BLOCK) as u64 + 2;

    let loops_in_duration = match bound {
        Bound::Timed(duration) => {
            BLOCKS_IN_ERA * duration.div(TIME_BETWEEN_BLOCKS.as_secs() as u32).as_secs() + 2
        }
        Bound::Counted(count) => count / events_per_loop + 2,
    };

    let finality_signatures_per_loop = NUMBER_OF_VALIDATORS as u64;
    let total_finality_signature_events = finality_signatures_per_loop * loops_in_duration;
    let deploy_events_per_loop = NUMBER_OF_DEPLOYS_PER_BLOCK as u64;
    let total_deploy_events = deploy_events_per_loop * loops_in_duration;
    let total_block_added_events = loops_in_duration;
    let total_step_events = loops_in_duration / BLOCKS_IN_ERA;

    let mut block_added_events = iter::repeat_with(|| SseData::random_block_added(test_rng))
        .take(plus_twenty_percent(total_block_added_events) as usize)
        .collect_vec();
    let mut deploy_accepted_events =
        iter::repeat_with(|| SseData::random_deploy_accepted(test_rng))
            .take(plus_twenty_percent(total_deploy_events) as usize)
            .collect_vec();
    let mut deploy_expired_events = iter::repeat_with(|| SseData::random_deploy_expired(test_rng))
        .take((loops_in_duration / 2 + 1) as usize)
        .collect_vec();
    let mut deploy_processed_events =
        iter::repeat_with(|| SseData::random_deploy_processed(test_rng))
            .take(plus_twenty_percent(total_deploy_events) as usize)
            .collect_vec();
    let mut fault_events = iter::repeat_with(|| SseData::random_fault(test_rng))
        .take((loops_in_duration / 2 + 1) as usize)
        .collect_vec();
    let mut finality_signature_events =
        iter::repeat_with(|| SseData::random_finality_signature(test_rng))
            .take(plus_twenty_percent(total_finality_signature_events) as usize)
            .collect_vec();
    let mut step_events = iter::repeat_with(|| SseData::random_step(test_rng))
        .take(plus_twenty_percent(total_step_events) as usize)
        .collect_vec();

    let mut interval = tokio::time::interval(TIME_BETWEEN_BLOCKS);

    events_sender
        .send(SseData::ApiVersion(API_VERSION_1_4_0))
        .await
        .unwrap();
    let mut era_counter: u16 = 0;
    let mut events_sent = 0;
    'outer: loop {
        for _ in 0..BLOCKS_IN_ERA {
            interval.tick().await;
            match bound {
                Bound::Timed(duration) => {
                    if start.elapsed() >= duration {
                        break 'outer;
                    }
                }
                Bound::Counted(count) => {
                    if events_sent >= count {
                        break 'outer;
                    }
                }
            }

            // Prior to each BlockAdded emit FinalitySignatures
            for _ in 0..NUMBER_OF_VALIDATORS {
                events_sender
                    .send(finality_signature_events.pop().unwrap())
                    .await
                    .expect("Failed sending finality_signature_event");
            }
            events_sent += NUMBER_OF_VALIDATORS as u64;

            // Emit DeployProcessed events for the next BlockAdded
            for _ in 0..NUMBER_OF_DEPLOYS_PER_BLOCK {
                events_sender
                    .send(deploy_processed_events.pop().unwrap())
                    .await
                    .expect("Failed sending deploy_processed_events");
            }
            events_sent += NUMBER_OF_DEPLOYS_PER_BLOCK as u64;

            // Emit the BlockAdded
            events_sender
                .send(block_added_events.pop().unwrap())
                .await
                .expect("Failed sending block_added_event");
            events_sent += 1;

            // Emit DeployAccepted Events
            for _ in 0..NUMBER_OF_DEPLOYS_PER_BLOCK {
                events_sender
                    .send(deploy_accepted_events.pop().unwrap().0)
                    .await
                    .expect("Failed sending deploy_accepted_event");
            }
            events_sent += NUMBER_OF_DEPLOYS_PER_BLOCK as u64;
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
        events_sent += 1;

        // Then a Step
        events_sender
            .send(step_events.pop().unwrap())
            .await
            .expect("Failed sending step_event");
        events_sent += 1;

        era_counter += 1;
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
        Bound::Counted(count) => {
            let mut events_sent = 0;
            while events_sent <= count {
                event_sender
                    .send(SseData::random_step(test_rng))
                    .await
                    .expect("Bound::Counted Failed sending random_step");
                events_sent += 1;
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
        Bound::Counted(count) => {
            let mut events_sent = 0;
            while events_sent <= count {
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
                events_sent += burst_size * 2;
            }
        }
    }
}

pub fn setup_mock_build_version_server(port: u16) -> () {
    let _ = setup_mock_build_version_server_with_version(port, "1.4.10".to_string());
}

pub fn setup_mock_build_version_server_with_version(
    port: u16,
    initial_version: String,
) -> Sender<String> {
    let m = Arc::new(Mutex::new(initial_version));
    let m1 = m.clone();
    let (tx, mut rx) = mpsc_channel(20);
    let version_store = warp::any().map(move || m1.clone());

    let api = warp::path!("status")
        .and(version_store.clone())
        .and_then(get_version);
    let server = warp::serve(api).run(([127, 0, 0, 1], port));
    tokio::spawn(async move {
        server.await;
        println!("terminating api version server");
    });
    tokio::spawn(async move {
        while let Some(ver) = rx.recv().await {
            let mut v = m.lock().unwrap();
            *v = ver;
            drop(v);
        }
    });
    tx
}

pub fn status_1_0_0_server(port: u16) -> Sender<String> {
    setup_mock_build_version_server_with_version(port, "1.0.0".to_string())
}

async fn get_version(version: Arc<Mutex<String>>) -> Result<impl warp::Reply, warp::Rejection> {
    let v = version.lock().unwrap();

    let result = json!({ "build_version": v.deref() });
    Ok(warp::reply::json(&result))
}
