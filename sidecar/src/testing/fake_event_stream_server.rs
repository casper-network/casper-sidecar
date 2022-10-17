use std::time::Duration;

use casper_event_types::SseData;
use casper_types::{testing::TestRng, ProtocolVersion};

use tempfile::TempDir;

use crate::event_stream_server::{Config as SseConfig, EventStreamServer};

const DEFAULT_NUM_OF_EVENT_BURSTS: u8 = 100;

pub enum EventStreamScenario {
    Realistic,
    LoadTestingStep,
    LoadTestingDeploy,
}

pub async fn spin_up_fake_event_stream(port: u16, scenario: EventStreamScenario) {
    let temp_dir = TempDir::new().expect("Error creating temporary directory");

    let event_stream_server = EventStreamServer::new(
        SseConfig::new(port, None, None),
        temp_dir.path().to_path_buf(),
        ProtocolVersion::V1_0_0,
    )
    .expect("Error spinning up Event Stream Server");

    match scenario {
        EventStreamScenario::Realistic => {
            tokio::spawn(realistic_event_streaming(event_stream_server));
        }
        EventStreamScenario::LoadTestingStep => {}
        EventStreamScenario::LoadTestingDeploy => {}
    }
}

async fn realistic_event_streaming(server: EventStreamServer) {
    let mut test_rng = TestRng::new();

    for _ in 0..15 {
        tokio::time::sleep(Duration::from_millis(500)).await;

        let deploy_expired = SseData::random_deploy_expired(&mut test_rng);
        server.broadcast(deploy_expired);
        for _ in 0..=5 {
            let fin_sig = SseData::random_finality_signature(&mut test_rng);
            server.broadcast(fin_sig);
        }
        let block_added = SseData::random_block_added(&mut test_rng);
        server.broadcast(block_added);
        for _ in 0..=3 {
            let deploy_accepted = SseData::random_deploy_accepted(&mut test_rng);
            server.broadcast(deploy_accepted.0);
        }
        let fault = SseData::random_fault(&mut test_rng);
        server.broadcast(fault);
        for _ in 0..=5 {
            let fin_sig = SseData::random_finality_signature(&mut test_rng);
            server.broadcast(fin_sig);
        }
        for _ in 0..=3 {
            let deploy_processed = SseData::random_deploy_processed(&mut test_rng);
            server.broadcast(deploy_processed);
        }
        let block_added = SseData::random_block_added(&mut test_rng);
        server.broadcast(block_added);
        let step = SseData::random_step(&mut test_rng);
        server.broadcast(step);
    }

    server.broadcast(SseData::Shutdown);
}
