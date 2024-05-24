use self::sse_server::LEGACY_ENDPOINT_NOTICE;

use super::*;
use casper_event_types::legacy_sse_data::LegacySseData;
use casper_types::{testing::TestRng, ProtocolVersion};
use futures::{join, Stream, StreamExt};
use http::StatusCode;
use pretty_assertions::assert_eq;
use reqwest::Response;
use serde_json::Value;
use sse_server::{
    Id, TransactionAccepted, QUERY_FIELD, SSE_API_DEPLOYS_PATH as DEPLOYS_PATH,
    SSE_API_MAIN_PATH as MAIN_PATH, SSE_API_ROOT_PATH as ROOT_PATH,
    SSE_API_SIGNATURES_PATH as SIGS_PATH,
};
use std::{
    collections::HashMap,
    error::Error,
    fs, io, iter,
    pin::Pin,
    str,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tempfile::TempDir;
use tokio::{
    sync::{Barrier, Notify},
    task::{self, JoinHandle},
    time::{self, timeout},
};
use tracing::debug;

/// The total number of random events each `EventStreamServer` will emit by default, excluding the
/// initial `ApiVersion` event.
const EVENT_COUNT: u32 = 100;
/// The maximum number of random events each `EventStreamServer` will emit, excluding the initial
/// `ApiVersion` event.
const MAX_EVENT_COUNT: u32 = 100_000_000;
/// The event stream buffer length, set in the server's config.  Set to half of the total event
/// count to allow for the buffer purging events in the test.
const BUFFER_LENGTH: u32 = EVENT_COUNT / 2;
/// The maximum amount of time to wait for a test server to complete.  If this time is exceeded, the
/// test has probably hung, and should be deemed to have failed.
const MAX_TEST_TIME: Duration = Duration::from_secs(10);
/// The duration of the sleep called between each event being sent by the server.
const DELAY_BETWEEN_EVENTS: Duration = Duration::from_millis(1);

type FilterLambda = Box<dyn Fn(u128, &SseData) -> Option<ReceivedEvent>>;

/// A helper to allow the synchronization of a single client joining the SSE server.
///
/// It provides the primitives to allow the client to connect to the server just before a specific
/// event is emitted by the server.
#[derive(Clone)]
struct ClientSyncBehavior {
    /// The event ID before which the server should wait at the barrier for the client to join.
    join_before_event: Id,
    /// The barrier to sync the client joining the server.
    barrier: Arc<Barrier>,
}

impl ClientSyncBehavior {
    fn new(join_before_event: Id) -> (Self, Arc<Barrier>) {
        let barrier = Arc::new(Barrier::new(2));
        let behavior = ClientSyncBehavior {
            join_before_event,
            barrier: Arc::clone(&barrier),
        };
        (behavior, barrier)
    }
}

/// A helper defining the behavior of the server.
#[derive(Clone)]
struct ServerBehavior {
    /// Whether the server should have a delay between sending events, to allow a client to keep up
    /// and not be disconnected for lagging.
    has_delay_between_events: bool,
    /// Whether the server should send all events once, or keep repeating the batch up until
    /// `MAX_EVENT_COUNT` have been sent.
    repeat_events: bool,
    /// If `Some`, sets the `max_concurrent_subscribers` server config value, otherwise uses the
    /// config default.
    max_concurrent_subscribers: Option<u32>,
    clients: Vec<ClientSyncBehavior>,
}

impl ServerBehavior {
    /// Returns a default new `ServerBehavior`.
    ///
    /// It has a small delay between events, and sends the collection of random events once.
    fn new() -> Self {
        ServerBehavior {
            has_delay_between_events: true,
            repeat_events: false,
            max_concurrent_subscribers: None,
            clients: Vec::new(),
        }
    }

    /// Returns a new `ServerBehavior` suitable for testing lagging clients.
    ///
    /// It has no delay between events, and sends the collection of random events repeatedly up to a
    /// maximum of `MAX_EVENT_COUNT` events.
    fn new_for_lagging_test() -> Self {
        ServerBehavior {
            has_delay_between_events: false,
            repeat_events: true,
            max_concurrent_subscribers: None,
            clients: Vec::new(),
        }
    }

    /// Adds a client sync behavior, specified for the client to connect to the server just before
    /// `id` is emitted.
    fn add_client_sync_before_event(&mut self, id: Id) -> Arc<Barrier> {
        let (client_behavior, barrier) = ClientSyncBehavior::new(id);
        self.clients.push(client_behavior);
        barrier
    }

    /// Sets the `max_concurrent_subscribers` server config value.
    fn set_max_concurrent_subscribers(&mut self, count: u32) {
        self.max_concurrent_subscribers = Some(count);
    }

    /// Waits for all clients which specified they wanted to join just before the given event ID.
    async fn wait_for_clients(&self, id: Id) {
        for client_behavior in &self.clients {
            if client_behavior.join_before_event == id {
                debug!("server waiting before event {}", id);
                timeout(Duration::from_secs(60), client_behavior.barrier.wait())
                    .await
                    .unwrap();
                debug!("server waiting for client to connect before event {}", id);
                timeout(Duration::from_secs(60), client_behavior.barrier.wait())
                    .await
                    .unwrap();
                debug!("server finished waiting before event {}", id);
            }
        }
    }

    /// Sleeps if `self` was set to enable delays between events.
    async fn sleep_if_required(&self) {
        if self.has_delay_between_events {
            time::sleep(DELAY_BETWEEN_EVENTS).await;
        } else {
            task::yield_now().await;
        }
    }
}

/// A helper to allow the server to be kept alive until a specific call to stop it.
#[derive(Clone)]
struct ServerStopper {
    should_stop: Arc<AtomicBool>,
    notifier: Arc<Notify>,
}

impl ServerStopper {
    fn new() -> Self {
        ServerStopper {
            should_stop: Arc::new(AtomicBool::new(false)),
            notifier: Arc::new(Notify::new()),
        }
    }

    /// Returns whether the server should stop now or not.
    fn should_stop(&self) -> bool {
        self.should_stop.load(Ordering::SeqCst)
    }

    /// Waits until the server should stop.
    async fn wait(&self) {
        while !self.should_stop() {
            self.notifier.notified().await;
        }
    }

    /// Tells the server to stop.
    fn stop(&self) {
        self.should_stop.store(true, Ordering::SeqCst);
        self.notifier.notify_one();
    }
}

impl Drop for ServerStopper {
    fn drop(&mut self) {
        self.stop();
    }
}

struct TestFixture {
    storage_dir: TempDir,
    protocol_version: ProtocolVersion,
    events: Vec<SseData>,
    first_event_id: Id,
    server_join_handle: Option<JoinHandle<()>>,
    server_stopper: ServerStopper,
}

impl TestFixture {
    /// Constructs a new `TestFixture` including `EVENT_COUNT` random events ready to be served.
    fn new(rng: &mut TestRng) -> Self {
        const DISTINCT_EVENTS_COUNT: u32 = 7;

        let storage_dir = tempfile::tempdir().unwrap();
        fs::create_dir_all(&storage_dir).unwrap();
        let protocol_version = ProtocolVersion::from_parts(1, 2, 3);

        let mut transactions = HashMap::new();
        let events: Vec<SseData> = (0..EVENT_COUNT)
            .map(|i| match i % DISTINCT_EVENTS_COUNT {
                0 => SseData::random_block_added(rng),
                1 => {
                    let (event, transaction) = SseData::random_transaction_accepted(rng);
                    assert!(transactions
                        .insert(transaction.hash(), transaction)
                        .is_none());
                    event
                }
                2 => SseData::random_transaction_processed(rng),
                3 => SseData::random_transaction_expired(rng),
                4 => SseData::random_fault(rng),
                5 => SseData::random_step(rng),
                6 => SseData::random_finality_signature(rng),
                _ => unreachable!(),
            })
            .collect();
        TestFixture {
            storage_dir,
            protocol_version,
            events,
            first_event_id: 0,
            server_join_handle: None,
            server_stopper: ServerStopper::new(),
        }
    }

    /// Creates a new `EventStreamServer` and runs it in a tokio task, returning the actual address
    /// the server is listening on.
    ///
    /// Only one server can be run at a time; this panics if there is already a server task running.
    ///
    /// The server emits a clone of each of the random events held by the `TestFixture`, in the
    /// order in which they're held in the `TestFixture`.
    ///
    /// The server runs until `TestFixture::stop_server()` is called, or the `TestFixture` is
    /// dropped.
    #[allow(clippy::too_many_lines)]
    async fn run_server(&mut self, server_behavior: ServerBehavior) -> SocketAddr {
        if self.server_join_handle.is_some() {
            panic!("one `TestFixture` can only run one server at a time");
        }
        self.server_stopper = ServerStopper::new();

        // Set the server to use a channel buffer of half the total events it will emit, unless
        // we're running with no delay between events, in which case set a minimal buffer as we're
        // trying to cause clients to get ejected for lagging.
        let config = Config {
            event_stream_buffer_length: if server_behavior.has_delay_between_events {
                BUFFER_LENGTH
            } else {
                1
            },
            max_concurrent_subscribers: server_behavior
                .max_concurrent_subscribers
                .unwrap_or(Config::default().max_concurrent_subscribers),
            ..Default::default()
        };
        let mut server =
            EventStreamServer::new(config, Some(self.storage_dir.path().to_path_buf()), true)
                .unwrap();

        self.first_event_id = server.event_indexer.current_index();

        let first_event_id = server.event_indexer.current_index();
        let server_address = server.listening_address;
        let events = self.events.clone();
        let server_stopper = self.server_stopper.clone();
        let protocol_version = self.protocol_version;
        let join_handle = tokio::spawn(async move {
            let event_count = if server_behavior.repeat_events {
                MAX_EVENT_COUNT
            } else {
                EVENT_COUNT
            };
            let api_version_event = SseData::ApiVersion(protocol_version);

            server.broadcast(api_version_event.clone(), Some(SseFilter::Events));
            for (id, event) in events.iter().cycle().enumerate().take(event_count as usize) {
                if server_stopper.should_stop() {
                    debug!("stopping server early");
                    return;
                }
                server_behavior
                    .wait_for_clients((id as Id).wrapping_add(first_event_id))
                    .await;
                server.broadcast(event.clone(), Some(SseFilter::Events));
                server_behavior.sleep_if_required().await;
            }

            // Keep the server running until told to stop.  Clients connecting from now will only
            // receive keepalives.
            debug!("server finished sending all events");
            server_stopper.wait().await;
            debug!("server stopped");
        });

        self.server_join_handle = Some(join_handle);

        server_address
    }

    /// Stops the currently-running server, if any, panicking if unable to stop the server within
    /// `MAX_TEST_TIME`.
    ///
    /// Must be called and awaited before starting a new server with this particular `TestFixture`.
    ///
    /// Should be called in every test where a server has been started, since this will ensure
    /// failed tests won't hang indefinitely.
    async fn stop_server(&mut self) {
        let join_handle = match self.server_join_handle.take() {
            Some(join_handle) => join_handle,
            None => return,
        };
        self.server_stopper.stop();
        time::timeout(MAX_TEST_TIME, join_handle)
            .await
            .expect("stopping server timed out (test hung)")
            .expect("server task should not error");
    }

    /// Returns all the events which would have been received by a client via
    /// `/events/<final_path_element>`, where the client connected just before `from` was emitted
    /// from the server.  This includes the initial `ApiVersion` event.
    ///
    /// Also returns the last event's ID,
    fn filtered_events(&self, final_path_element: &str, from: Id) -> (Vec<ReceivedEvent>, Id) {
        // Convert the IDs to `u128`s to cater for wrapping and add `Id::MAX + 1` to `from` if the
        // buffer wrapped and `from` represents an event from after the wrap.
        let threshold = Id::MAX - EVENT_COUNT;
        let from = if self.first_event_id >= threshold && from < threshold {
            from as u128 + Id::MAX as u128 + 1
        } else {
            from as u128
        };

        let api_version_event = ReceivedEvent {
            id: None,
            data: serde_json::to_string(&SseData::ApiVersion(self.protocol_version)).unwrap(),
        };
        let id_filter = build_id_filter(from);
        let (filter, _is_legacy_filter) = sse_server::get_filter(final_path_element, true).unwrap();
        let events: Vec<ReceivedEvent> = iter::once(api_version_event)
            .chain(
                self.events
                    .iter()
                    .filter(|event| !matches!(event, SseData::ApiVersion(..)))
                    .enumerate()
                    .filter_map(|(id, event)| {
                        let id = id as u128 + self.first_event_id as u128;
                        if event.should_include(filter) {
                            id_filter(id, event)
                        } else {
                            None
                        }
                    }),
            )
            .collect();
        let final_id = events
            .last()
            .expect("should have events")
            .id
            .expect("should have ID");
        (events, final_id)
    }

    /// Returns all the events which would have been received by a client connected from server
    /// startup via `/events/<final_path_element>`, including the initial `ApiVersion` event.
    ///
    /// Also returns the last event's ID.
    fn all_filtered_events(&self, final_path_element: &str) -> (Vec<ReceivedEvent>, Id) {
        self.filtered_events(final_path_element, self.first_event_id)
    }
}

/// Returns the URL for a client to use to connect to the server at the given address.
///
/// The URL is `/events/<final_path_element>` with `?start_from=X` query string appended if
/// `maybe_start_from` is `Some`.
fn url(
    server_address: SocketAddr,
    final_path_element: &str,
    maybe_start_from: Option<Id>,
) -> String {
    format!(
        "http://{}/{}/{}{}",
        server_address,
        ROOT_PATH,
        final_path_element,
        match maybe_start_from {
            Some(start_from) => format!("?{}={}", QUERY_FIELD, start_from),
            None => String::new(),
        }
    )
}

/// The representation of an SSE event as received by a subscribed client.
#[derive(Clone, Debug, Eq)]
struct ReceivedEvent {
    id: Option<Id>,
    data: String,
}

/// We used to compare ReceivedEvents `.data` by string equality, but it seems that after changes with sending json_data to outbound
/// serde json produces a json object with alphabetically ordered fields for serde_json::Value. For structs it retains the order in which the fields are defined.
/// Obviously json is field-order agnostic - so the json objects we get in ReceivedEvents might denote the same json object, but they are
/// not the same string. Hence we need to deserialize `.data` and compare structures.
impl PartialEq for ReceivedEvent {
    fn eq(&self, other: &Self) -> bool {
        let this_data = serde_json::from_str::<Value>(&self.data).unwrap();
        let that_data = serde_json::from_str::<Value>(&other.data).unwrap();
        self.id.eq(&other.id) && this_data.eq(&that_data)
    }
}

/// Runs a client, consuming all SSE events until the server has emitted the event with ID
/// `final_event_id`.
///
/// If the client receives a keepalive (i.e. `:`), it panics, as the server has no further events to
/// emit.
///
/// The client waits at the barrier before connecting to the server, and then again immediately
/// after connecting to ensure the server doesn't start sending events before the client is
/// connected.
async fn subscribe(
    url: &str,
    barrier: Arc<Barrier>,
    final_event_id: Id,
    client_id: &str,
) -> Result<Vec<ReceivedEvent>, reqwest::Error> {
    timeout(Duration::from_secs(60), barrier.wait())
        .await
        .unwrap();
    let response = reqwest::get(url).await?;
    timeout(Duration::from_secs(60), barrier.wait())
        .await
        .unwrap();
    handle_response(response, final_event_id, client_id).await
}

// Similar to the `subscribe()` function, except this has a long pause at the start and short
// pauses after each read.
//
// The objective is to create backpressure by filling the client's receive buffer, then filling
// the server's send buffer, which in turn causes the server's internal broadcast channel to
// deem that client as lagging.
async fn subscribe_slow(
    url: &str,
    barrier: Arc<Barrier>,
    client_id: &str,
) -> Result<(), reqwest::Error> {
    timeout(Duration::from_secs(60), barrier.wait())
        .await
        .unwrap();
    let response = reqwest::get(url).await.unwrap();
    timeout(Duration::from_secs(60), barrier.wait())
        .await
        .unwrap();
    time::sleep(Duration::from_secs(5)).await;

    let mut stream = response.bytes_stream();

    let pause_between_events = Duration::from_secs(100) / MAX_EVENT_COUNT;
    let mut bytes_buf: Vec<u8> = vec![];
    while let Some(item) = stream.next().await {
        // The function is expected to exit here with an `UnexpectedEof` error.
        let bytes = item?;
        // We fetch bytes untill we get a chunk that can be fully interpreted as utf-8
        let res: Vec<u8> = [bytes_buf.as_slice(), bytes.as_ref()].concat();
        match str::from_utf8(res.as_slice()) {
            Ok(chunk) => {
                if chunk.lines().any(|line| line == ":") {
                    debug!("{} received keepalive: exiting", client_id);
                    break;
                }
                bytes_buf = vec![];
            }
            Err(_) => {
                bytes_buf = res;
            }
        }
        time::sleep(pause_between_events).await;
    }
    Ok(())
}

/// Runs a client, consuming all SSE events until the server has emitted the event with ID
/// `final_event_id`.
///
/// If the client receives a keepalive (i.e. `:`), it panics, as the server has no further events to
/// emit.
///
/// There is no synchronization between client and server regarding the client joining.  In most
/// tests such synchronization is required, in which case `subscribe()` should be used.
async fn subscribe_no_sync(
    url: &str,
    final_event_id: Id,
    client_id: &str,
) -> Result<Vec<ReceivedEvent>, reqwest::Error> {
    debug!("{} about to connect via {}", client_id, url);
    let response = reqwest::get(url).await?;
    debug!("{} has connected", client_id);
    handle_response(response, final_event_id, client_id).await
}

/// Handles a response from the server.
async fn handle_response(
    response: Response,
    final_event_id: Id,
    client_id: &str,
) -> Result<Vec<ReceivedEvent>, reqwest::Error> {
    if response.status() == StatusCode::SERVICE_UNAVAILABLE {
        debug!("{} rejected by server: too many clients", client_id);
        assert_eq!(
            response.text().await.unwrap(),
            "server has reached limit of subscribers"
        );
        return Ok(Vec::new());
    }

    let stream = response.bytes_stream();
    let final_id_line = format!("id:{}", final_event_id);
    let keepalive = ":";

    let response_text = fetch_text(
        Box::pin(stream),
        final_id_line,
        keepalive,
        client_id,
        final_event_id,
    )
    .await?;

    Ok(parse_response(response_text, client_id))
}

async fn fetch_text(
    mut stream: Pin<Box<dyn Stream<Item = Result<bytes::Bytes, reqwest::Error>> + Send + 'static>>,
    final_id_line: String,
    keepalive: &str,
    client_id: &str,
    final_event_id: u32,
) -> Result<String, reqwest::Error> {
    let mut bytes_buf: Vec<u8> = vec![];
    let mut response_text = String::new();
    // The stream from the server is not always chunked into events, so gather the stream into a
    // single `String` until we receive a keepalive. Furthermore - a chunk of bytes can even split a utf8 character in half
    // which makes it impossible to interpret it as utf8. We need to fetch bytes untill we get a chunk that can be fully interpreted as utf-8
    while let Some(item) = stream.next().await {
        let bytes = item?;
        // We fetch bytes untill we get a chunk that can be fully interpreted as utf-8
        let res: Vec<u8> = [bytes_buf.as_slice(), bytes.as_ref()].concat();
        match str::from_utf8(res.as_slice()) {
            Ok(chunk) => {
                response_text.push_str(chunk);
                if let Some(line) = response_text
                    .lines()
                    .find(|&line| line == final_id_line || line == keepalive)
                {
                    if line == keepalive {
                        panic!("{} received keepalive", client_id);
                    }
                    debug!(
                        "{} received final event ID {}: exiting",
                        client_id, final_event_id
                    );
                    return Ok(response_text);
                }
                bytes_buf = vec![];
            }
            Err(_) => {
                bytes_buf = res;
            }
        }
    }
    Ok(response_text)
}

/// Iterate the lines of the response body.  Each line should be one of
///   * an SSE event: line starts with "data:" and the remainder of the line is a JSON object
///   * an SSE event ID: line starts with "id:" and the remainder is a decimal encoded `u32`
///   * empty
///   * a keepalive: line contains exactly ":"
///
/// The expected order is:
///   * data:<JSON-encoded ApiVersion> (note, no ID line follows this first event)
/// then the following three repeated for as many events as are applicable to that stream:
///   * data:<JSON-encoded event>
///   * id:<integer>
///   * empty line
/// then finally, repeated keepalive lines until the server is shut down.
#[allow(clippy::too_many_lines)]
fn parse_response(response_text: String, client_id: &str) -> Vec<ReceivedEvent> {
    let mut received_events = Vec::new();
    let mut line_itr = response_text.lines();
    let mut first_line = true;
    while let Some(data_line) = line_itr.next() {
        let data = match data_line.strip_prefix("data:") {
            Some(data_str) => data_str.to_string(),
            None => {
                if first_line {
                    // In legacy endpoints the first line is a comment containing deprecation notice. When we remove the legacy endpoints we can remove this check.
                    if data_line.trim() == format!(":{}", LEGACY_ENDPOINT_NOTICE) {
                        continue;
                    }
                    first_line = false;
                }
                if data_line.trim().is_empty() || data_line.trim() == ":" {
                    continue;
                } else {
                    panic!(
                        "{}: data line should start with 'data:'\n{}",
                        client_id, data_line
                    )
                }
            }
        };

        let id_line = match line_itr.next() {
            Some(line) => line,
            None => break,
        };

        let id = match id_line.strip_prefix("id:") {
            Some(id_str) => Some(id_str.parse().unwrap_or_else(|_| {
                panic!("{}: failed to get ID line from:\n{}", client_id, id_line)
            })),
            None => {
                if id_line.trim().is_empty() && received_events.is_empty() {
                    None
                } else if id_line.trim() == ":" {
                    continue;
                } else {
                    panic!(
                        "{}: every event must have an ID except the first one",
                        client_id
                    );
                }
            }
        };

        received_events.push(ReceivedEvent { id, data });
    }
    received_events
}

/// Client setup:
///   * `<IP:port>/events/<path>`
///   * no `?start_from=` query
///   * connected before first event
///
/// Expected to receive all main, transaction-accepted or signature events depending on `filter`.
async fn should_serve_events_with_no_query(path: &str, is_legacy_endpoint: bool) {
    let mut rng = TestRng::new();
    let mut fixture = TestFixture::new(&mut rng);

    let mut server_behavior = ServerBehavior::new();
    let barrier = server_behavior.add_client_sync_before_event(0);
    let server_address = fixture.run_server(server_behavior).await;

    let url = url(server_address, path, None);
    let (expected_events, final_id) = fixture.all_filtered_events(path);
    let (expected_events, final_id) =
        adjust_final_id(is_legacy_endpoint, expected_events, final_id);
    let received_events = subscribe(&url, barrier, final_id, "client").await.unwrap();
    fixture.stop_server().await;
    compare_received_events_for_legacy_endpoints(
        is_legacy_endpoint,
        expected_events,
        received_events,
    );
}

/// In legacy endpoints not all input events will be re-emitted to output. If an input (2.x) event is not translatable
/// to 1.x it will be muffled. So we need to adjust the final id to the last event that was 1.x translatable.
fn adjust_final_id(
    is_legacy_endpoint: bool,
    expected_events: Vec<ReceivedEvent>,
    final_id: u32,
) -> (Vec<ReceivedEvent>, u32) {
    let (expected_events, final_id) = if is_legacy_endpoint {
        let legacy_compliant_events: Vec<ReceivedEvent> = expected_events
            .iter()
            .filter_map(|event| {
                let sse_data = serde_json::from_str::<SseData>(&event.data).unwrap();
                LegacySseData::from(&sse_data).map(|_| event.clone())
            })
            .collect();
        let id = legacy_compliant_events.last().and_then(|el| el.id).unwrap();
        (legacy_compliant_events, id)
    } else {
        (expected_events, final_id)
    };
    (expected_events, final_id)
}

/// In legacy endpoints the node produces 2.x compliant sse events, but the node transforms them into legacy format.
/// So to compare we need to apply the translation logic to input 2.x events.
fn compare_received_events_for_legacy_endpoints(
    is_legacy_endpoint: bool,
    expected_events: Vec<ReceivedEvent>,
    received_events: Vec<ReceivedEvent>,
) {
    if is_legacy_endpoint {
        let expected_legacy_events: Vec<LegacySseData> = expected_events
            .iter()
            .filter_map(|event| {
                let sse_data = serde_json::from_str::<SseData>(&event.data).unwrap();
                LegacySseData::from(&sse_data)
            })
            .collect();
        let received_legacy_events: Vec<LegacySseData> = received_events
            .iter()
            .map(|event| serde_json::from_str::<LegacySseData>(&event.data).unwrap())
            .collect();
        assert_eq!(received_legacy_events, expected_legacy_events);
    } else {
        assert_eq!(received_events, expected_events);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn should_serve_main_events_with_no_query() {
    should_serve_events_with_no_query(MAIN_PATH, true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn should_serve_deploy_accepted_events_with_no_query() {
    should_serve_events_with_no_query(DEPLOYS_PATH, true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn should_serve_signature_events_with_no_query() {
    should_serve_events_with_no_query(SIGS_PATH, true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn should_serve_firehose_events_with_no_query() {
    should_serve_events_with_no_query(ROOT_PATH, false).await;
}

/// Client setup:
///   * `<IP:port>/events/<path>?start_from=25`
///   * connected just before event ID 50
///
/// Expected to receive main, transaction-accepted or signature events (depending on `path`) from ID 25
/// onwards, as events 25 to 49 should still be in the server buffer.
async fn should_serve_events_with_query(path: &str, is_legacy_endpoint: bool) {
    let mut rng = TestRng::new();
    let mut fixture = TestFixture::new(&mut rng);

    let connect_at_event_id = BUFFER_LENGTH;
    let start_from_event_id = BUFFER_LENGTH / 2;

    let mut server_behavior = ServerBehavior::new();
    let barrier = server_behavior.add_client_sync_before_event(connect_at_event_id);
    let server_address = fixture.run_server(server_behavior).await;

    let url = url(server_address, path, Some(start_from_event_id));
    let (expected_events, final_id) = fixture.filtered_events(path, start_from_event_id);
    let (expected_events, final_id) =
        adjust_final_id(is_legacy_endpoint, expected_events, final_id);
    let received_events = subscribe(&url, barrier, final_id, "client").await.unwrap();
    fixture.stop_server().await;

    compare_received_events_for_legacy_endpoints(
        is_legacy_endpoint,
        expected_events,
        received_events,
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn should_serve_main_events_with_query() {
    should_serve_events_with_query(MAIN_PATH, true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn should_serve_deploy_accepted_events_with_query() {
    should_serve_events_with_query(DEPLOYS_PATH, true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn should_serve_signature_events_with_query() {
    should_serve_events_with_query(SIGS_PATH, true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn should_serve_firehose_events_with_query() {
    should_serve_events_with_query(ROOT_PATH, false).await;
}

/// Client setup:
///   * `<IP:port>/events/<path>?start_from=0`
///   * connected just before event ID 75
///
/// Expected to receive main, transaction-accepted or signature events (depending on `path`) from ID 25
/// onwards, as events 0 to 24 should have been purged from the server buffer.
async fn should_serve_remaining_events_with_query(path: &str, is_legacy_endpoint: bool) {
    let mut rng = TestRng::new();
    let mut fixture = TestFixture::new(&mut rng);

    let connect_at_event_id = BUFFER_LENGTH * 3 / 2;
    let start_from_event_id = 0;

    let mut server_behavior = ServerBehavior::new();
    let barrier = server_behavior.add_client_sync_before_event(connect_at_event_id);
    let server_address = fixture.run_server(server_behavior).await;

    let url = url(server_address, path, Some(start_from_event_id));
    let expected_first_event = connect_at_event_id - BUFFER_LENGTH;
    let (expected_events, final_id) = fixture.filtered_events(path, expected_first_event);
    let (expected_events, final_id) =
        adjust_final_id(is_legacy_endpoint, expected_events, final_id);
    let received_events = subscribe(&url, barrier, final_id, "client").await.unwrap();
    fixture.stop_server().await;

    compare_received_events_for_legacy_endpoints(
        is_legacy_endpoint,
        expected_events,
        received_events,
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn should_serve_remaining_main_events_with_query() {
    should_serve_remaining_events_with_query(MAIN_PATH, true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn should_serve_remaining_deploy_accepted_events_with_query() {
    should_serve_remaining_events_with_query(DEPLOYS_PATH, true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn should_serve_remaining_signature_events_with_query() {
    should_serve_remaining_events_with_query(SIGS_PATH, true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn should_serve_remaining_firehose_events_with_query() {
    should_serve_remaining_events_with_query(ROOT_PATH, false).await;
}

/// Client setup:
///   * `<IP:port>/events/<path>?start_from=25`
///   * connected before first event
///
/// Expected to receive all main, transaction-accepted or signature events (depending on `path`), as
/// event 25 hasn't been added to the server buffer yet.
async fn should_serve_events_with_query_for_future_event(path: &str, is_legacy_endpoint: bool) {
    let mut rng = TestRng::new();
    let mut fixture = TestFixture::new(&mut rng);

    let mut server_behavior = ServerBehavior::new();
    let barrier = server_behavior.add_client_sync_before_event(0);
    let server_address = fixture.run_server(server_behavior).await;

    let url = url(server_address, path, Some(25));
    let (expected_events, final_id) = fixture.all_filtered_events(path);
    let (expected_events, final_id) =
        adjust_final_id(is_legacy_endpoint, expected_events, final_id);
    let received_events = subscribe(&url, barrier, final_id, "client").await.unwrap();
    fixture.stop_server().await;

    compare_received_events_for_legacy_endpoints(
        is_legacy_endpoint,
        expected_events,
        received_events,
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn should_serve_main_events_with_query_for_future_event() {
    should_serve_events_with_query_for_future_event(MAIN_PATH, true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn should_serve_deploy_accepted_events_with_query_for_future_event() {
    should_serve_events_with_query_for_future_event(DEPLOYS_PATH, true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn should_serve_signature_events_with_query_for_future_event() {
    should_serve_events_with_query_for_future_event(SIGS_PATH, true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn should_serve_firehose_events_with_query_for_future_event() {
    should_serve_events_with_query_for_future_event(ROOT_PATH, false).await;
}

/// Checks that when a server is shut down (e.g. for a node upgrade), connected clients don't have
/// an error while handling the HTTP response.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn server_exit_should_gracefully_shut_down_stream() {
    let mut rng = TestRng::new();
    let mut fixture = TestFixture::new(&mut rng);

    // Start the server, waiting for three clients to connect.
    let mut server_behavior = ServerBehavior::new();
    let barrier1 = server_behavior.add_client_sync_before_event(0);
    let server_address = fixture.run_server(server_behavior).await;

    let url1 = url(server_address, ROOT_PATH, None);

    // Run the three clients, and stop the server after a short delay.
    let (received_events1, _) = join!(subscribe(&url1, barrier1, EVENT_COUNT, "client 1"), async {
        time::sleep(DELAY_BETWEEN_EVENTS * EVENT_COUNT / 2).await;
        fixture.stop_server().await
    });

    // Ensure all clients' streams terminated without error.
    let received_events1 = received_events1.unwrap();

    // Ensure all clients received some events...
    assert!(!received_events1.is_empty());

    // ...but not the full set they would have if the server hadn't stopped early.
    assert!(received_events1.len() < fixture.all_filtered_events(ROOT_PATH).0.len());
}

/// Checks that clients which don't consume the events in a timely manner are forcibly disconnected
/// by the server.
#[allow(clippy::too_many_lines)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lagging_clients_should_be_disconnected() {
    let mut rng = TestRng::new();
    let mut fixture = TestFixture::new(&mut rng);

    // Start the server, setting it to run with no delay between sending each event.  It will send
    // at most `MAX_EVENT_COUNT` events, but the clients' futures should return before that, having
    // been disconnected for lagging.
    let mut server_behavior = ServerBehavior::new_for_lagging_test();
    let barrier_events = server_behavior.add_client_sync_before_event(0);
    let server_address = fixture.run_server(server_behavior).await;

    let url_events = url(server_address, ROOT_PATH, None);

    // Run the slow clients, then stop the server.
    let result_slow_events = subscribe_slow(&url_events, barrier_events, "client 1").await;
    fixture.stop_server().await;
    // Ensure both slow clients' streams terminated with an `UnexpectedEof` error.
    let check_error = |result: Result<(), reqwest::Error>| {
        let kind = result
            .unwrap_err()
            .source()
            .expect("reqwest::Error should have source")
            .downcast_ref::<hyper::Error>()
            .expect("reqwest::Error's source should be a hyper::Error")
            .source()
            .expect("hyper::Error should have source")
            .downcast_ref::<io::Error>()
            .expect("hyper::Error's source should be a std::io::Error")
            .kind();
        assert!(matches!(kind, io::ErrorKind::UnexpectedEof));
    };
    check_error(result_slow_events);
}

/// Checks that clients using the correct <IP:Port> but wrong path get a helpful error response.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn should_handle_bad_url_path() {
    let mut rng = TestRng::new();
    let mut fixture = TestFixture::new(&mut rng);

    let server_address = fixture.run_server(ServerBehavior::new()).await;

    #[rustfmt::skip]
    let urls = [
        format!("http://{}", server_address),
        format!("http://{}?{}=0", server_address, QUERY_FIELD),
        format!("http://{}/bad", server_address),
        format!("http://{}/bad?{}=0", server_address, QUERY_FIELD),
        format!("http://{}/{}?{}=0", server_address, QUERY_FIELD, ROOT_PATH),
        format!("http://{}/{}/bad", server_address, ROOT_PATH),
        format!("http://{}/{}/bad?{}=0", server_address, QUERY_FIELD, ROOT_PATH),
    ];

    let expected_body = "invalid path: expected '/events/main', '/events/deploys' or '/events/sigs or '/events/sidecar'";
    for url in &urls {
        let response = reqwest::get(url).await.unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND, "URL: {}", url);
        assert_eq!(
            response.text().await.unwrap().trim(),
            expected_body,
            "URL: {}",
            url
        );
    }

    fixture.stop_server().await;
}

async fn start_query_url_test() -> (TestFixture, SocketAddr) {
    let mut rng = TestRng::new();
    let mut fixture = TestFixture::new(&mut rng);
    let server_address = fixture.run_server(ServerBehavior::new()).await;
    (fixture, server_address)
}

fn build_urls(server_address: SocketAddr) -> String {
    format!("http://{}/{}", server_address, ROOT_PATH)
}
/// Checks that clients using the correct <IP:Port/path> but wrong query get a helpful error
/// response.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn should_handle_bad_url_query() {
    let (mut fixture, server_address) = start_query_url_test().await;
    let events_url = build_urls(server_address);
    let urls = [
        format!("{}?not-a-kv-pair", events_url),
        format!("{}?start_fro=0", events_url),
        format!("{}?{}=not-integer", events_url, QUERY_FIELD),
        format!("{}?{}='0'", events_url, QUERY_FIELD),
        format!("{}?{}=0&extra=1", events_url, QUERY_FIELD),
    ];
    let expected_body = format!(
        "invalid query: expected single field '{}=<EVENT ID>'",
        QUERY_FIELD
    );
    for url in &urls {
        let response = reqwest::get(url).await.unwrap();
        assert_eq!(
            response.status(),
            StatusCode::UNPROCESSABLE_ENTITY,
            "URL: {}",
            url
        );
        assert_eq!(
            response.text().await.unwrap().trim(),
            &expected_body,
            "URL: {}",
            url
        );
    }
    fixture.stop_server().await;
}

async fn subscribe_for_comment(
    url: &str,
    barrier: Arc<Barrier>,
    final_event_id: Id,
    client_id: &str,
) -> String {
    timeout(Duration::from_secs(60), barrier.wait())
        .await
        .unwrap();
    let response = reqwest::get(url).await.unwrap();
    timeout(Duration::from_secs(60), barrier.wait())
        .await
        .unwrap();
    let stream = response.bytes_stream();
    let final_id_line = format!("id:{}", final_event_id); // This theoretically is not optimal since we don't need to read all the events from the test,
                                                          // but it's not easy to determine what id is the first one in the stream for legacy tests.
    let keepalive = ":";
    fetch_text(
        Box::pin(stream),
        final_id_line,
        keepalive,
        client_id,
        final_event_id,
    )
    .await
    .unwrap()
}

async fn should_get_comment(path: &str) {
    let mut rng = TestRng::new();
    let mut fixture = TestFixture::new(&mut rng);
    let mut server_behavior = ServerBehavior::new();
    let barrier = server_behavior.add_client_sync_before_event(0);
    let server_address = fixture.run_server(server_behavior).await;

    // Consume these and stop the server.
    let url = url(server_address, path, None);
    let (expected_events, final_id) = fixture.all_filtered_events(path);
    let (_expected_events, final_id) = adjust_final_id(true, expected_events, final_id);
    let text = subscribe_for_comment(&url, barrier, final_id, "client 1").await;
    fixture.stop_server().await;
    let expected_start = format!(":{}", LEGACY_ENDPOINT_NOTICE);
    assert!(text.as_str().starts_with(expected_start.as_str()));
}

#[allow(clippy::too_many_lines)]
/// Check that a server which restarts continues from the previous numbering of event IDs.
async fn should_persist_event_ids(path: &str, is_legacy_endpoint: bool) {
    let mut rng = TestRng::new();
    let mut fixture = TestFixture::new(&mut rng);

    let first_run_final_id = {
        // Run the first server to emit the 100 events.
        let mut server_behavior = ServerBehavior::new();
        let barrier = server_behavior.add_client_sync_before_event(0);
        let server_address = fixture.run_server(server_behavior).await;

        // Consume these and stop the server.
        let url = url(server_address, path, None);
        let (expected_events, final_id) = fixture.all_filtered_events(path);
        let (_expected_events, final_id) =
            adjust_final_id(is_legacy_endpoint, expected_events, final_id);
        let _ = subscribe(&url, barrier, final_id, "client 1")
            .await
            .unwrap();
        fixture.stop_server().await;
        final_id
    };

    assert!(first_run_final_id > 0);
    {
        // Start a new server with a client barrier set for just before event ID 100 + 1 (the extra
        // event being the `Shutdown`).
        let mut server_behavior = ServerBehavior::new();
        let barrier = server_behavior.add_client_sync_before_event(EVENT_COUNT + 1);
        let server_address = fixture.run_server(server_behavior).await;

        // Check the test fixture has set the server's first event ID to at least
        // `first_run_final_id`.
        assert!(fixture.first_event_id >= first_run_final_id);

        // Consume the events and assert their IDs are all >= `first_run_final_id`.
        let url = url(server_address, path, None);
        let (expected_events, final_id) = fixture.filtered_events(path, EVENT_COUNT + 1);
        let (expected_events, final_id) =
            adjust_final_id(is_legacy_endpoint, expected_events, final_id);
        let received_events = subscribe(&url, barrier, final_id, "client 2")
            .await
            .unwrap();
        fixture.stop_server().await;
        assert!(received_events
            .iter()
            .skip(1)
            .all(|event| event.id.unwrap() >= first_run_final_id));
        compare_received_events_for_legacy_endpoints(
            is_legacy_endpoint,
            expected_events,
            received_events,
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn should_get_comment_on_first_message_in_deploys() {
    should_get_comment(DEPLOYS_PATH).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn should_get_comment_on_first_message_in_sigs() {
    should_get_comment(SIGS_PATH).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn should_get_comment_on_first_message_in_main() {
    should_get_comment(MAIN_PATH).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn should_persist_deploy_accepted_event_ids() {
    should_persist_event_ids(DEPLOYS_PATH, true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn should_persist_signature_event_ids() {
    should_persist_event_ids(SIGS_PATH, true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn should_persist_firehose_event_ids() {
    should_persist_event_ids(ROOT_PATH, false).await;
}

/// Check that a server handles wrapping round past the maximum value for event IDs.
async fn should_handle_wrapping_past_max_event_id(path: &str) {
    let mut rng = TestRng::new();
    let mut fixture = TestFixture::new(&mut rng);

    // Set up an `EventIndexer` cache file as if the server previously stopped at an event with ID
    // just less than the maximum.
    let start_index = Id::MAX - (BUFFER_LENGTH / 2);
    fs::write(
        fixture.storage_dir.path().join("sse_index"),
        start_index.to_le_bytes(),
    )
    .unwrap();

    // Set up a client which will connect at the start of the stream, and another two for once the
    // IDs have wrapped past the maximum value.
    let mut server_behavior = ServerBehavior::new();
    let barrier1 = server_behavior.add_client_sync_before_event(start_index);
    let barrier2 = server_behavior.add_client_sync_before_event(BUFFER_LENGTH / 2);
    let barrier3 = server_behavior.add_client_sync_before_event(BUFFER_LENGTH / 2);
    let server_address = fixture.run_server(server_behavior).await;
    assert_eq!(fixture.first_event_id, start_index);

    // The first client doesn't need a query string, but the second will request to start from an ID
    // from before they wrapped past the maximum value, and the third from event 0.
    let url1 = url(server_address, path, None);
    let url2 = url(server_address, path, Some(start_index + 1));
    let url3 = url(server_address, path, Some(0));
    let (expected_events1, final_id1) = fixture.all_filtered_events(path);
    let (expected_events2, final_id2) = fixture.filtered_events(path, start_index + 1);
    let (expected_events3, final_id3) = fixture.filtered_events(path, 0);
    let (received_events1, received_events2, received_events3) = join!(
        subscribe(&url1, barrier1, final_id1, "client 1"),
        subscribe(&url2, barrier2, final_id2, "client 2"),
        subscribe(&url3, barrier3, final_id3, "client 3"),
    );
    fixture.stop_server().await;

    assert_eq!(received_events1.unwrap(), expected_events1);
    assert_eq!(received_events2.unwrap(), expected_events2);
    assert_eq!(received_events3.unwrap(), expected_events3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn should_handle_wrapping_past_max_event_id_for_events() {
    should_handle_wrapping_past_max_event_id(ROOT_PATH).await;
}

/// Checks that a server rejects new clients with an HTTP 503 when it already has the specified
/// limit of connected clients.
#[allow(clippy::too_many_lines)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn should_limit_concurrent_subscribers() {
    let mut rng = TestRng::new();
    let mut fixture = TestFixture::new(&mut rng);

    // Start the server with `max_concurrent_subscribers == 4`, and set to wait for three clients to
    // connect at event 0 and another three at event 1.
    let mut server_behavior = ServerBehavior::new();
    server_behavior.set_max_concurrent_subscribers(1);
    let barrier1 = server_behavior.add_client_sync_before_event(0);
    let barrier5 = server_behavior.add_client_sync_before_event(1);
    let server_address = fixture.run_server(server_behavior).await;

    let url_root = url(server_address, ROOT_PATH, None);

    let (expected_events_root, final_id) = fixture.all_filtered_events(ROOT_PATH);

    // Run the six clients.
    let (received_events_1, empty_events_1) = join!(
        subscribe(&url_root, barrier1, final_id, "client 1"),
        subscribe(&url_root, barrier5, final_id, "client 2"),
    );

    // Check the first three received all expected events.
    assert_eq!(received_events_1.unwrap(), expected_events_root);

    // Check the second three received no events.
    assert!(empty_events_1.unwrap().is_empty());

    // Check that now the first clients have all disconnected, three new clients can connect.  Have
    // them start from event 80 to allow them to actually pull some events off the stream (as the
    // server has by now stopped creating any new events).
    let start_id = EVENT_COUNT - 20;

    let url_root = url(server_address, ROOT_PATH, Some(start_id));

    let (expected_root_events, final_root_id) = fixture.filtered_events(ROOT_PATH, start_id);

    let received_events_root = subscribe_no_sync(&url_root, final_root_id, "client 3").await;

    // Check the last three clients' received events are as expected.
    assert_eq!(received_events_root.unwrap(), expected_root_events);
    fixture.stop_server().await;
}

fn build_id_filter(from: u128) -> FilterLambda {
    Box::new(move |id: u128, event: &SseData| -> Option<ReceivedEvent> {
        if id < from {
            return None;
        }

        let data = match event {
            SseData::TransactionAccepted(transaction) => {
                serde_json::to_string(&TransactionAccepted {
                    transaction_accepted: transaction.clone(),
                })
                .unwrap()
            }
            _ => serde_json::to_string(event).unwrap(),
        };

        Some(ReceivedEvent {
            id: Some(id as Id),
            data,
        })
    })
}
