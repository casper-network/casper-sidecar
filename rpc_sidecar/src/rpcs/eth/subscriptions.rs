use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use casper_event_types::SidecarEvent;
use casper_json_rpc::{
    CorsOrigin, Error as RpcError, JsonRpcOptions, MethodLimiter, Notification, Params,
    RequestDispatcher, RequestHandlers, handle_json_request_bytes,
};
use futures::{SinkExt, StreamExt};
use metrics::rpc::{inc_method_call, observe_response_time, register_request_size};
use serde::Serialize;
use serde_json::{Value, json};
use tokio::{
    sync::{
        broadcast::{Receiver as BroadcastReceiver, Sender as BroadcastSender, error::RecvError},
        mpsc::{self, UnboundedSender},
        oneshot,
    },
    task::JoinHandle,
    time::sleep,
};
use tracing::{debug, info, warn};
use warp::{
    Filter, Rejection, Reply,
    filters::BoxedFilter,
    http::StatusCode,
    reject::Reject,
    reply,
    ws::{Message, WebSocket},
};

use super::{
    log_filter::{
        LogFilter, RawLogFilter, ensure_log_block_range_within_limit, latest_block_height,
        logs_for_block_height, logs_for_filter,
    },
    types::invalid_params,
};
use crate::rpcs::NodeClient;

const LOGS_SUBSCRIPTION: &str = "logs";
pub(crate) const SUBSCRIBE_METHOD: &str = "eth_subscribe";
pub(crate) const UNSUBSCRIBE_METHOD: &str = "eth_unsubscribe";
const LOG_FETCH_RETRY_ATTEMPTS: usize = 10;
const LOG_FETCH_RETRY_DELAY: Duration = Duration::from_secs(1);
const LOG_BACKFILL_BLOCK_DELAY: Duration = Duration::from_millis(25);

static NEXT_SUBSCRIPTION_ID: AtomicU64 = AtomicU64::new(1);

#[allow(clippy::too_many_arguments)]
pub(crate) fn websocket_route(
    api_path: &'static str,
    handlers: RequestHandlers,
    node_client: Arc<dyn NodeClient>,
    sidecar_event_sender: BroadcastSender<SidecarEvent>,
    json_rpc_options: JsonRpcOptions,
    origin_policy: WebSocketOriginPolicy,
    max_eth_log_block_range: u64,
    max_body_bytes: u64,
    subscribe_limiter: MethodLimiter,
    unsubscribe_limiter: MethodLimiter,
) -> BoxedFilter<(reply::Response,)> {
    let max_message_size = usize::try_from(max_body_bytes).unwrap_or(usize::MAX);
    warp::path::path(api_path)
        .and(warp::path::end())
        .and(warp::ws())
        .and(websocket_origin_filter(origin_policy))
        .map(move |ws: warp::ws::Ws| {
            let ws = ws.max_message_size(max_message_size);
            let handlers = handlers.clone();
            let node_client = node_client.clone();
            let sidecar_event_sender = sidecar_event_sender.clone();
            let subscribe_limiter = subscribe_limiter.clone();
            let unsubscribe_limiter = unsubscribe_limiter.clone();
            ws.on_upgrade(move |websocket| {
                handle_websocket(
                    websocket,
                    handlers,
                    node_client,
                    sidecar_event_sender,
                    json_rpc_options,
                    max_eth_log_block_range,
                    subscribe_limiter,
                    unsubscribe_limiter,
                )
            })
            .into_response()
        })
        .recover(handle_websocket_rejection)
        .unify()
        .boxed()
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum WebSocketOriginPolicy {
    NoBrowserOrigins,
    Any,
    Specified(String),
}

impl WebSocketOriginPolicy {
    pub(crate) fn from_cors_header(cors_header: Option<&CorsOrigin>) -> Self {
        match cors_header {
            Some(CorsOrigin::Any) => Self::Any,
            Some(CorsOrigin::Specified(origin)) => Self::Specified(origin.clone()),
            None => Self::NoBrowserOrigins,
        }
    }
}

#[derive(Debug)]
struct WebSocketOriginForbidden;

impl Reject for WebSocketOriginForbidden {}

fn websocket_origin_filter(policy: WebSocketOriginPolicy) -> BoxedFilter<()> {
    warp::header::optional::<String>("origin")
        .and_then(move |origin: Option<String>| {
            let policy = policy.clone();
            async move {
                if websocket_origin_allowed(&policy, origin.as_deref()) {
                    Ok(())
                } else {
                    Err(warp::reject::custom(WebSocketOriginForbidden))
                }
            }
        })
        .untuple_one()
        .boxed()
}

fn websocket_origin_allowed(policy: &WebSocketOriginPolicy, origin: Option<&str>) -> bool {
    match policy {
        WebSocketOriginPolicy::NoBrowserOrigins => origin.is_none(),
        WebSocketOriginPolicy::Any => true,
        WebSocketOriginPolicy::Specified(allowed_origin) => {
            origin.is_some_and(|origin| origin == allowed_origin)
        }
    }
}

async fn handle_websocket_rejection(error: Rejection) -> Result<reply::Response, Rejection> {
    if error.find::<WebSocketOriginForbidden>().is_some() {
        Ok(reply::with_status("Forbidden", StatusCode::FORBIDDEN).into_response())
    } else {
        Err(error)
    }
}

#[allow(clippy::too_many_arguments)]
async fn handle_websocket(
    websocket: WebSocket,
    handlers: RequestHandlers,
    node_client: Arc<dyn NodeClient>,
    sidecar_event_sender: BroadcastSender<SidecarEvent>,
    json_rpc_options: JsonRpcOptions,
    max_eth_log_block_range: u64,
    subscribe_limiter: MethodLimiter,
    unsubscribe_limiter: MethodLimiter,
) {
    info!("eth websocket connection opened");
    let (mut websocket_tx, mut websocket_rx) = websocket.split();
    let (outbound_tx, mut outbound_rx) = mpsc::unbounded_channel::<OutboundMessage>();
    let writer = tokio::spawn(async move {
        while let Some(outbound) = outbound_rx.recv().await {
            let message = match outbound {
                OutboundMessage::Json(value) => Message::text(value.to_string()),
                OutboundMessage::CloseTooLarge => {
                    Message::close_with(1009u16, "JSON-RPC message exceeds max_body_bytes")
                }
            };
            let is_close = message.is_close();
            if websocket_tx.send(message).await.is_err() {
                warn!("eth websocket writer failed; closing writer task");
                break;
            }
            if is_close {
                break;
            }
        }
    });

    let mut dispatcher = WebSocketDispatcher {
        handlers,
        node_client,
        sidecar_event_sender,
        outbound_tx: outbound_tx.clone(),
        subscriptions: HashMap::new(),
        pending_activations: Vec::new(),
        max_block_range: max_eth_log_block_range,
        subscribe_limiter,
        unsubscribe_limiter,
    };
    let mut sent_too_large_close = false;
    while let Some(message) = websocket_rx.next().await {
        let message = match message {
            Ok(message) => message,
            Err(error) => {
                if error.to_string().contains("Message too long") {
                    sent_too_large_close = outbound_tx.send(OutboundMessage::CloseTooLarge).is_ok();
                }
                break;
            }
        };
        if message.is_close() {
            break;
        }
        if !(message.is_text() || message.is_binary()) {
            continue;
        }

        let output =
            handle_json_request_bytes(message.as_bytes(), &mut dispatcher, &json_rpc_options).await;
        if let Some(value) = output.into_value()
            && outbound_tx.send(OutboundMessage::Json(value)).is_err()
        {
            break;
        }
        dispatcher.activate_pending();
    }

    dispatcher.abort_subscriptions();
    drop(dispatcher);
    drop(outbound_tx);
    if sent_too_large_close {
        let _ = writer.await;
    } else {
        writer.abort();
    }
    info!("eth websocket connection closed");
}

enum OutboundMessage {
    Json(Value),
    CloseTooLarge,
}

type OutboundSender = UnboundedSender<OutboundMessage>;

struct WebSocketDispatcher {
    handlers: RequestHandlers,
    node_client: Arc<dyn NodeClient>,
    sidecar_event_sender: BroadcastSender<SidecarEvent>,
    outbound_tx: OutboundSender,
    subscriptions: HashMap<String, JoinHandle<()>>,
    pending_activations: Vec<oneshot::Sender<()>>,
    max_block_range: u64,
    subscribe_limiter: MethodLimiter,
    unsubscribe_limiter: MethodLimiter,
}

impl RequestDispatcher for WebSocketDispatcher {
    async fn dispatch(
        &mut self,
        method: &str,
        params: Option<Params>,
        request_size: usize,
    ) -> Result<Value, RpcError> {
        if method != SUBSCRIBE_METHOD && method != UNSUBSCRIBE_METHOD {
            return self.handlers.dispatch(method, params, request_size).await;
        }

        let start = Instant::now();
        inc_method_call(method);
        register_request_size(method, request_size);
        let limiter = if method == SUBSCRIBE_METHOD {
            self.subscribe_limiter.clone()
        } else {
            self.unsubscribe_limiter.clone()
        };
        let result = match limiter.check() {
            Err(error) => Err(error),
            Ok(()) if method == SUBSCRIBE_METHOD => self.subscribe(params).await,
            Ok(()) => self.unsubscribe(params),
        };
        let status = result
            .as_ref()
            .map_or_else(|error| error.code().to_string(), |_| "success".to_string());
        observe_response_time(method, &status, start.elapsed());
        result
    }
}

impl WebSocketDispatcher {
    async fn subscribe(&mut self, params: Option<Params>) -> Result<Value, RpcError> {
        let filter = parse_subscribe_filter(params)?;
        let subscription_start =
            prepare_log_subscription_start(self.node_client.clone(), &filter, self.max_block_range)
                .await?;

        let subscription_id = next_subscription_id();

        let notification_tx = self.outbound_tx.clone();
        let subscription_node_client = self.node_client.clone();
        let subscription_filter = filter.clone();
        let subscription_id_for_task = subscription_id.clone();
        let sidecar_event_receiver = self.sidecar_event_sender.subscribe();
        let max_block_range = self.max_block_range;
        let (activation_tx, activation_rx) = oneshot::channel();
        info!(
            subscription_id,
            filter = ?filter,
            active_subscriptions = self.subscriptions.len() + 1,
            sidecar_event_receivers = self.sidecar_event_sender.receiver_count(),
            "created eth logs subscription"
        );
        let handle = tokio::spawn(async move {
            if activation_rx.await.is_err() {
                return;
            }
            run_log_subscription(
                subscription_node_client,
                subscription_filter,
                subscription_id_for_task,
                sidecar_event_receiver,
                notification_tx,
                subscription_start,
                max_block_range,
            )
            .await;
        });
        self.subscriptions.insert(subscription_id.clone(), handle);
        self.pending_activations.push(activation_tx);
        Ok(json!(subscription_id))
    }

    fn unsubscribe(&mut self, params: Option<Params>) -> Result<Value, RpcError> {
        let subscription_id = parse_unsubscribe_id(params)?;
        let existed = self
            .subscriptions
            .remove(&subscription_id)
            .map(|handle| {
                handle.abort();
                true
            })
            .unwrap_or(false);
        info!(subscription_id, existed, "eth unsubscribe requested");
        Ok(json!(existed))
    }

    fn activate_pending(&mut self) {
        for activation in self.pending_activations.drain(..) {
            let _ = activation.send(());
        }
    }

    fn abort_subscriptions(&mut self) {
        self.pending_activations.clear();
        for (_, subscription) in self.subscriptions.drain() {
            subscription.abort();
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct LogSubscriptionStart {
    latest_height: Option<u64>,
    finite_to_block: Option<u64>,
    initial_range: Option<(u64, u64)>,
    next_height: u64,
}

async fn prepare_log_subscription_start(
    node_client: Arc<dyn NodeClient>,
    filter: &LogFilter,
    max_block_range: u64,
) -> Result<Option<LogSubscriptionStart>, RpcError> {
    if filter.block_hash().is_some() {
        return Ok(None);
    }

    let latest_height = latest_block_height(node_client).await?;
    let finite_to_block = filter.finite_to_block_height()?;
    let (initial_range, next_height) = initial_subscription_range(filter, latest_height)?;
    if let Some((from_height, to_height)) = initial_range {
        ensure_log_block_range_within_limit(from_height, to_height, max_block_range)?;
    }

    Ok(Some(LogSubscriptionStart {
        latest_height,
        finite_to_block,
        initial_range,
        next_height,
    }))
}

async fn run_log_subscription(
    node_client: Arc<dyn NodeClient>,
    filter: LogFilter,
    subscription_id: String,
    mut sidecar_event_receiver: BroadcastReceiver<SidecarEvent>,
    outbound_tx: OutboundSender,
    subscription_start: Option<LogSubscriptionStart>,
    max_block_range: u64,
) {
    if filter.block_hash().is_some() {
        info!(
            subscription_id,
            filter = ?filter,
            "starting block-hash eth logs subscription"
        );
        match logs_for_filter_with_retries(node_client, &filter, max_block_range).await {
            Ok(logs) => {
                info!(
                    subscription_id,
                    log_count = logs.len(),
                    "fetched block-hash eth logs subscription"
                );
                for log in logs {
                    send_notification(&outbound_tx, &subscription_id, log);
                }
            }
            Err(error) => {
                warn!(?error, "failed to fetch logs for block-hash subscription");
            }
        }
        futures::future::pending::<()>().await;
        return;
    }

    let Some(subscription_start) = subscription_start else {
        warn!(
            subscription_id,
            "missing prepared eth logs subscription start"
        );
        return;
    };
    let latest_height = subscription_start.latest_height;
    let finite_to_block = subscription_start.finite_to_block;
    let initial_range = subscription_start.initial_range;
    let mut next_height = subscription_start.next_height;
    info!(
        subscription_id,
        latest_height,
        finite_to_block,
        initial_range = ?initial_range,
        next_height,
        filter = ?filter,
        "starting eth logs subscription task"
    );

    if let Some((from_height, to_height)) = initial_range {
        match send_logs_for_block_range_with_retries(
            node_client.clone(),
            &filter,
            from_height,
            to_height,
            &subscription_id,
            &outbound_tx,
            max_block_range,
        )
        .await
        {
            Ok(outcome) => {
                info!(
                    subscription_id,
                    from_height,
                    to_height,
                    emitted_logs = outcome.emitted_logs,
                    next_height = outcome.next_height,
                    "finished initial eth logs subscription backfill"
                );
                next_height = outcome.next_height;
            }
            Err(error) => {
                warn!(
                    error = ?error.error,
                    from_height, to_height, "failed to fetch initial log subscription range"
                );
                next_height = error.next_height;
                if error.fatal {
                    return;
                }
            }
        }
    }

    if subscription_is_complete(next_height, finite_to_block) {
        info!(
            subscription_id,
            next_height, finite_to_block, "eth logs subscription completed after initial range"
        );
        return;
    }

    loop {
        tokio::select! {
            event = sidecar_event_receiver.recv() => {
                match event {
                    Ok(SidecarEvent::BlockAdded { block }) => {
                        let height = block.height();
                        info!(
                            subscription_id,
                            event_height = height,
                            next_height,
                            "eth logs subscription received BlockAdded event"
                        );
                        let Some((from_height, to_height, next_after_event)) =
                            block_range_after_event(next_height, height, finite_to_block)
                        else {
                            debug!(
                                subscription_id,
                                event_height = height,
                                next_height,
                                finite_to_block,
                                "eth logs subscription skipped old BlockAdded event"
                            );
                            continue;
                        };
                        match send_logs_for_block_range_with_retries(
                            node_client.clone(),
                            &filter,
                            from_height,
                            to_height,
                            &subscription_id,
                            &outbound_tx,
                            max_block_range,
                        )
                        .await
                        {
                            Ok(outcome) => {
                                debug_assert_eq!(outcome.next_height, next_after_event);
                                info!(
                                    subscription_id,
                                    from_height,
                                    to_height,
                                    emitted_logs = outcome.emitted_logs,
                                    next_height = outcome.next_height,
                                    "processed eth logs subscription BlockAdded range"
                                );
                                next_height = outcome.next_height;
                            }
                            Err(error) => {
                                warn!(
                                    error = ?error.error,
                                    from_height, to_height, "failed to fetch logs for new block range"
                                );
                                next_height = error.next_height;
                                if error.fatal {
                                    return;
                                }
                            }
                        }
                    }
                    Err(RecvError::Lagged(_)) => {
                        warn!(
                            subscription_id,
                            next_height,
                            "eth logs subscription lagged on sidecar event receiver"
                        );
                        let Ok(Some(latest_height)) = latest_block_height(node_client.clone()).await else {
                            warn!(
                                subscription_id,
                                "failed to read latest block height after lagged sidecar event receiver"
                            );
                            continue;
                        };
                        info!(
                            subscription_id,
                            latest_height,
                            next_height,
                            "eth logs subscription recovering from lagged sidecar event receiver"
                        );
                        let Some((from_height, to_height, next_after_event)) =
                            block_range_after_event(next_height, latest_height, finite_to_block)
                        else {
                            debug!(
                                subscription_id,
                                latest_height,
                                next_height,
                                finite_to_block,
                                "eth logs subscription lag recovery found no new range"
                            );
                            continue;
                        };
                        match send_logs_for_block_range_with_retries(
                            node_client.clone(),
                            &filter,
                            from_height,
                            to_height,
                            &subscription_id,
                            &outbound_tx,
                            max_block_range,
                        )
                        .await
                        {
                            Ok(outcome) => {
                                debug_assert_eq!(outcome.next_height, next_after_event);
                                info!(
                                    subscription_id,
                                    from_height,
                                    to_height,
                                    emitted_logs = outcome.emitted_logs,
                                    next_height = outcome.next_height,
                                    "processed eth logs subscription lag recovery range"
                                );
                                next_height = outcome.next_height;
                            }
                            Err(error) => {
                                warn!(
                                    error = ?error.error,
                                    from_height, to_height, "failed to catch up lagged log subscription"
                                );
                                next_height = error.next_height;
                                if error.fatal {
                                    return;
                                }
                            }
                        }
                    }
                    Err(RecvError::Closed) => {
                        warn!(
                            subscription_id,
                            "eth logs subscription sidecar event channel closed"
                        );
                        break;
                    }
                    Ok(_) => {}
                }
            }
        }

        if subscription_is_complete(next_height, finite_to_block) {
            info!(
                subscription_id,
                next_height, finite_to_block, "eth logs subscription completed"
            );
            return;
        }
    }
}

fn initial_subscription_range(
    filter: &LogFilter,
    latest_height: Option<u64>,
) -> Result<(Option<(u64, u64)>, u64), RpcError> {
    let Some(latest_height) = latest_height else {
        return Ok((None, 0));
    };

    if !filter.has_block_range_bound() {
        return Ok((None, latest_height.saturating_add(1)));
    }

    let from_height = filter.from_block_height_or_latest(latest_height)?;
    let to_height = filter.to_block_height(latest_height)?.min(latest_height);
    if from_height > to_height {
        Ok((None, from_height))
    } else {
        Ok((Some((from_height, to_height)), to_height.saturating_add(1)))
    }
}

fn block_range_after_event(
    next_height: u64,
    event_height: u64,
    finite_to_block: Option<u64>,
) -> Option<(u64, u64, u64)> {
    let to_height = finite_to_block
        .map(|max_height| event_height.min(max_height))
        .unwrap_or(event_height);
    if to_height < next_height {
        None
    } else {
        Some((next_height, to_height, to_height.saturating_add(1)))
    }
}

fn subscription_is_complete(next_height: u64, finite_to_block: Option<u64>) -> bool {
    finite_to_block.is_some_and(|to_block| next_height > to_block)
}

#[derive(Debug)]
struct LogRangeFetchError {
    error: RpcError,
    next_height: u64,
    fatal: bool,
}

#[derive(Debug)]
struct LogRangeFetchOutcome {
    next_height: u64,
    emitted_logs: usize,
}

async fn logs_for_filter_with_retries(
    node_client: Arc<dyn NodeClient>,
    filter: &LogFilter,
    max_block_range: u64,
) -> Result<Vec<super::projection::LogResponse>, RpcError> {
    let mut attempts_remaining = LOG_FETCH_RETRY_ATTEMPTS;
    loop {
        match logs_for_filter(node_client.clone(), filter, max_block_range).await {
            Ok(logs) => return Ok(logs),
            Err(error) if attempts_remaining > 0 => {
                attempts_remaining -= 1;
                warn!(
                    ?error,
                    attempts_remaining, "retrying log subscription filter fetch"
                );
                sleep(LOG_FETCH_RETRY_DELAY).await;
            }
            Err(error) => return Err(error),
        }
    }
}

async fn send_logs_for_block_range_with_retries(
    node_client: Arc<dyn NodeClient>,
    filter: &LogFilter,
    from_height: u64,
    to_height: u64,
    subscription_id: &str,
    outbound_tx: &OutboundSender,
    max_block_range: u64,
) -> Result<LogRangeFetchOutcome, LogRangeFetchError> {
    if let Err(error) = ensure_log_block_range_within_limit(from_height, to_height, max_block_range)
    {
        return Err(LogRangeFetchError {
            error,
            next_height: from_height,
            fatal: true,
        });
    }

    let mut next_height = from_height;
    let mut emitted_logs = 0usize;
    for height in from_height..=to_height {
        let logs =
            match logs_for_block_height_with_retries(node_client.clone(), filter, height).await {
                Ok(logs) => logs,
                Err(error) => {
                    return Err(LogRangeFetchError {
                        error,
                        next_height,
                        fatal: false,
                    });
                }
            };
        let log_count = logs.len();
        if log_count > 0 {
            info!(
                subscription_id,
                height, log_count, "eth logs subscription matched block logs"
            );
        } else {
            debug!(
                subscription_id,
                height, "eth logs subscription found no matching logs in block"
            );
        }
        for log in logs {
            send_notification(outbound_tx, subscription_id, log);
        }
        emitted_logs = emitted_logs.saturating_add(log_count);
        next_height = height.saturating_add(1);
        if height < to_height {
            // Backfills can touch many blocks. Pace them so a ranged
            // `eth_subscribe` request does not exhaust the node binary-port
            // one-second request window before live block events arrive.
            sleep(LOG_BACKFILL_BLOCK_DELAY).await;
        }
    }
    Ok(LogRangeFetchOutcome {
        next_height,
        emitted_logs,
    })
}

async fn logs_for_block_height_with_retries(
    node_client: Arc<dyn NodeClient>,
    filter: &LogFilter,
    height: u64,
) -> Result<Vec<super::projection::LogResponse>, RpcError> {
    let mut attempts_remaining = LOG_FETCH_RETRY_ATTEMPTS;
    loop {
        match logs_for_block_height(node_client.clone(), filter, height).await {
            Ok(logs) => return Ok(logs),
            Err(error) if attempts_remaining > 0 => {
                attempts_remaining -= 1;
                warn!(
                    ?error,
                    attempts_remaining, height, "retrying log subscription block fetch"
                );
                sleep(LOG_FETCH_RETRY_DELAY).await;
            }
            Err(error) => return Err(error),
        }
    }
}

fn parse_subscribe_filter(params: Option<Params>) -> Result<LogFilter, RpcError> {
    let params = match params {
        Some(Params::Array(params)) => params,
        _ => return Err(invalid_params("'params' must be an array")),
    };
    let subscription_kind = params
        .first()
        .and_then(Value::as_str)
        .ok_or_else(|| invalid_params("missing subscription type"))?;
    if subscription_kind != LOGS_SUBSCRIPTION {
        return Err(invalid_params(format!(
            "unsupported subscription type: {subscription_kind}"
        )));
    }
    if params.len() > 2 {
        return Err(invalid_params(
            "'eth_subscribe' accepts a subscription type and optional filter object",
        ));
    }
    let raw_filter = match params.get(1) {
        Some(filter) => {
            serde_json::from_value::<RawLogFilter>(filter.clone()).map_err(|error| {
                invalid_params(format!("failed to parse log subscription filter: {error}"))
            })?
        }
        None => RawLogFilter::default(),
    };
    LogFilter::try_from(raw_filter)
}

fn parse_unsubscribe_id(params: Option<Params>) -> Result<String, RpcError> {
    let params = match params {
        Some(Params::Array(params)) => params,
        _ => return Err(invalid_params("'params' must be an array")),
    };
    if params.len() != 1 {
        return Err(invalid_params(
            "'eth_unsubscribe' accepts exactly one subscription id",
        ));
    }
    params[0]
        .as_str()
        .map(str::to_string)
        .ok_or_else(|| invalid_params("subscription id must be a string"))
}

fn send_notification(
    outbound_tx: &OutboundSender,
    subscription_id: &str,
    log: super::projection::LogResponse,
) {
    debug!(
        subscription_id,
        block_number = %log.block_number,
        transaction_hash = %log.transaction_hash,
        log_index = %log.log_index,
        "sending eth subscription log notification"
    );
    #[derive(Serialize)]
    struct SubscriptionParams<'a> {
        subscription: &'a str,
        result: super::projection::LogResponse,
    }

    let notification = Notification::new(
        "eth_subscription",
        SubscriptionParams {
            subscription: subscription_id,
            result: log,
        },
    );
    let notification =
        serde_json::to_value(notification).expect("subscription notification should serialize");

    if outbound_tx
        .send(OutboundMessage::Json(notification))
        .is_err()
    {
        warn!(
            subscription_id,
            "failed to enqueue eth subscription notification; websocket writer is closed"
        );
    }
}

fn next_subscription_id() -> String {
    let id = NEXT_SUBSCRIPTION_ID.fetch_add(1, Ordering::Relaxed);
    format!("0x{id:x}")
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use casper_binary_port::InformationRequest;
    use casper_json_rpc::{ConfigLimit, MethodLimiter, RequestHandlersBuilder};
    use casper_types::{
        Block, BlockSignatures, BlockWithSignatures, TestBlockBuilder, testing::TestRng,
    };
    use serde_json::json;
    use tokio::time::{Duration, timeout};

    use super::*;
    use crate::rpcs::test_utils::BinaryPortMock;

    fn test_websocket_route(
        calls: Arc<AtomicUsize>,
        max_body_bytes: u64,
    ) -> BoxedFilter<(reply::Response,)> {
        let mut handlers = RequestHandlersBuilder::new();
        handlers.register_handler(
            "echo",
            move |params| {
                let calls = calls.clone();
                async move {
                    calls.fetch_add(1, Ordering::Relaxed);
                    Ok::<_, RpcError>(params.map(Value::from).unwrap_or(Value::Null))
                }
            },
            &ConfigLimit::default(),
        );
        let (sidecar_event_sender, _) = tokio::sync::broadcast::channel(16);
        websocket_route(
            "rpc",
            handlers.build(),
            Arc::new(BinaryPortMock::new()),
            sidecar_event_sender,
            JsonRpcOptions::default(),
            WebSocketOriginPolicy::NoBrowserOrigins,
            10_000,
            max_body_bytes,
            MethodLimiter::new(&ConfigLimit::default()),
            MethodLimiter::new(&ConfigLimit::default()),
        )
    }

    #[tokio::test]
    async fn websocket_processes_binary_batches_and_suppresses_notification_frames() {
        let calls = Arc::new(AtomicUsize::new(0));
        let route = test_websocket_route(calls.clone(), 10_000);
        let mut websocket = warp::test::ws()
            .path("/rpc")
            .handshake(route)
            .await
            .unwrap();

        websocket
            .send(Message::binary(
                serde_json::to_vec(&json!([
                    {"jsonrpc":"2.0","method":"echo","params":[1],"id":1},
                    {"jsonrpc":"2.0","method":"echo","params":[2]},
                    true
                ]))
                .unwrap(),
            ))
            .await;
        let message = websocket.recv().await.unwrap();
        let response: Value = serde_json::from_slice(message.as_bytes()).unwrap();
        assert!(response.is_array());
        assert_eq!(response.as_array().unwrap().len(), 2);
        assert_eq!(response[0]["result"], json!([1]));
        assert_eq!(response[1]["error"]["code"], -32600);
        assert_eq!(calls.load(Ordering::Relaxed), 2);

        websocket
            .send_text(json!({"jsonrpc":"2.0","method":"echo","params":[3]}).to_string())
            .await;
        assert!(
            timeout(Duration::from_millis(50), websocket.recv())
                .await
                .is_err()
        );
        assert_eq!(calls.load(Ordering::Relaxed), 3);

        websocket
            .send_text(json!({"jsonrpc":"2.0","method":"echo","id":null}).to_string())
            .await;
        let response: Value =
            serde_json::from_slice(websocket.recv().await.unwrap().as_bytes()).unwrap();
        assert_eq!(response["id"], Value::Null);
        assert_eq!(response["result"], Value::Null);
    }

    #[tokio::test]
    async fn websocket_closes_oversized_messages() {
        let route = test_websocket_route(Arc::new(AtomicUsize::new(0)), 64);
        let mut websocket = warp::test::ws()
            .path("/rpc")
            .handshake(route)
            .await
            .unwrap();
        websocket.send_text("x".repeat(65)).await;
        websocket.recv_closed().await.unwrap();
    }

    #[tokio::test]
    async fn websocket_dispatches_subscribe_and_unsubscribe_inside_a_batch() {
        let client = Arc::new(BinaryPortMock::new());
        let rng = &mut TestRng::new();
        let block = Block::V2(TestBlockBuilder::new().height(10).build(rng));
        client
            .add_block_with_signatures(
                BlockWithSignatures::new(block, BlockSignatures::random(rng)),
                InformationRequest::BlockWithSignatures(None),
            )
            .await;
        let (sidecar_event_sender, _) = tokio::sync::broadcast::channel(16);
        let route = websocket_route(
            "rpc",
            RequestHandlersBuilder::new().build(),
            client,
            sidecar_event_sender,
            JsonRpcOptions::default(),
            WebSocketOriginPolicy::NoBrowserOrigins,
            10_000,
            10_000,
            MethodLimiter::new(&ConfigLimit::default()),
            MethodLimiter::new(&ConfigLimit::default()),
        );
        let mut websocket = warp::test::ws()
            .path("/rpc")
            .handshake(route)
            .await
            .unwrap();

        websocket
            .send_text(
                json!([
                    {"jsonrpc":"2.0","method":"eth_subscribe","params":["logs"],"id":1},
                    {"jsonrpc":"2.0","method":"eth_unsubscribe","params":["missing"],"id":2}
                ])
                .to_string(),
            )
            .await;
        let response: Value =
            serde_json::from_slice(websocket.recv().await.unwrap().as_bytes()).unwrap();
        assert!(
            response[0]["result"]
                .as_str()
                .is_some_and(|id| id.starts_with("0x"))
        );
        assert_eq!(response[1]["result"], false);
    }

    #[tokio::test]
    async fn subscription_notification_creates_subscription_without_direct_response() {
        let client = Arc::new(BinaryPortMock::new());
        let rng = &mut TestRng::new();
        let block = Block::V2(TestBlockBuilder::new().height(10).build(rng));
        client
            .add_block_with_signatures(
                BlockWithSignatures::new(block, BlockSignatures::random(rng)),
                InformationRequest::BlockWithSignatures(None),
            )
            .await;
        let (outbound_tx, mut outbound_rx) = mpsc::unbounded_channel();
        let (sidecar_event_sender, _) = tokio::sync::broadcast::channel(16);
        let mut dispatcher = WebSocketDispatcher {
            handlers: RequestHandlersBuilder::new().build(),
            node_client: client,
            sidecar_event_sender,
            outbound_tx,
            subscriptions: HashMap::new(),
            pending_activations: Vec::new(),
            max_block_range: 10_000,
            subscribe_limiter: MethodLimiter::new(&ConfigLimit::default()),
            unsubscribe_limiter: MethodLimiter::new(&ConfigLimit::default()),
        };

        let body = serde_json::to_vec(&json!({
            "jsonrpc": "2.0",
            "method": "eth_subscribe",
            "params": ["logs"],
        }))
        .unwrap();
        let output =
            handle_json_request_bytes(&body, &mut dispatcher, &JsonRpcOptions::default()).await;
        assert_eq!(output, casper_json_rpc::JsonRpcOutput::NoResponse);
        assert_eq!(dispatcher.subscriptions.len(), 1);
        assert_eq!(dispatcher.pending_activations.len(), 1);
        assert!(outbound_rx.try_recv().is_err());
        dispatcher.abort_subscriptions();
    }

    #[tokio::test]
    async fn activation_barrier_queues_batch_response_before_subscription_events() {
        let (outbound_tx, mut outbound_rx) = mpsc::unbounded_channel();
        let (sidecar_event_sender, _) = tokio::sync::broadcast::channel(16);
        let mut dispatcher = WebSocketDispatcher {
            handlers: RequestHandlersBuilder::new().build(),
            node_client: Arc::new(BinaryPortMock::new()),
            sidecar_event_sender,
            outbound_tx: outbound_tx.clone(),
            subscriptions: HashMap::new(),
            pending_activations: Vec::new(),
            max_block_range: 10_000,
            subscribe_limiter: MethodLimiter::new(&ConfigLimit::default()),
            unsubscribe_limiter: MethodLimiter::new(&ConfigLimit::default()),
        };
        let (activation_tx, activation_rx) = oneshot::channel();
        dispatcher.pending_activations.push(activation_tx);
        let notification_tx = outbound_tx.clone();
        let task = tokio::spawn(async move {
            activation_rx.await.unwrap();
            notification_tx
                .send(OutboundMessage::Json(json!({"event": true})))
                .unwrap();
        });

        outbound_tx
            .send(OutboundMessage::Json(
                json!([{"result": "subscription-id"}]),
            ))
            .unwrap();
        dispatcher.activate_pending();

        let Some(OutboundMessage::Json(first)) = outbound_rx.recv().await else {
            panic!("expected queued batch response");
        };
        let Some(OutboundMessage::Json(second)) = outbound_rx.recv().await else {
            panic!("expected activated subscription event");
        };
        assert_eq!(first, json!([{"result": "subscription-id"}]));
        assert_eq!(second, json!({"event": true}));
        task.await.unwrap();
    }

    #[test]
    fn parses_log_subscription_filter() {
        let filter = parse_subscribe_filter(Some(Params::Array(vec![
            json!("logs"),
            json!({ "topics": [null] }),
        ])))
        .unwrap();

        assert!(filter.block_hash().is_none());
    }

    #[test]
    fn cast_log_subscription_filter_backfills_requested_range() {
        let filter = parse_subscribe_filter(Some(Params::Array(vec![
            json!("logs"),
            json!({
                "fromBlock": "earliest",
                "toBlock": "latest",
                "address": "0x0000000000000000000000000000000000000001",
                "topics": [
                    "0x59950fb23669ee30425f6d79758e75fae698a6c88b2982f2980638d8bcd9397d"
                ]
            }),
        ])))
        .unwrap();

        assert_eq!(
            initial_subscription_range(&filter, Some(12)).unwrap(),
            (Some((0, 12)), 13)
        );
    }

    #[test]
    fn unbounded_log_subscription_starts_after_latest_block() {
        let filter = parse_subscribe_filter(Some(Params::Array(vec![
            json!("logs"),
            json!({ "topics": [null] }),
        ])))
        .unwrap();

        assert_eq!(
            initial_subscription_range(&filter, Some(12)).unwrap(),
            (None, 13)
        );
    }

    #[test]
    fn rejects_unsupported_subscription_kind() {
        let err = parse_subscribe_filter(Some(Params::Array(vec![json!("newHeads")]))).unwrap_err();

        assert_eq!(
            err,
            invalid_params("unsupported subscription type: newHeads")
        );
    }

    #[test]
    fn parses_unsubscribe_id() {
        let subscription_id =
            parse_unsubscribe_id(Some(Params::Array(vec![json!("0x1")]))).unwrap();

        assert_eq!(subscription_id, "0x1");
    }

    #[test]
    fn websocket_origin_policy_follows_cors_config() {
        let no_browser_origins = WebSocketOriginPolicy::from_cors_header(None);
        assert!(websocket_origin_allowed(&no_browser_origins, None));
        assert!(!websocket_origin_allowed(
            &no_browser_origins,
            Some("https://example.com")
        ));

        let any_origin = WebSocketOriginPolicy::from_cors_header(Some(&CorsOrigin::Any));
        assert!(websocket_origin_allowed(&any_origin, None));
        assert!(websocket_origin_allowed(
            &any_origin,
            Some("https://example.com")
        ));

        let specified_origin = WebSocketOriginPolicy::from_cors_header(Some(
            &CorsOrigin::Specified("https://allowed.example".to_string()),
        ));
        assert!(websocket_origin_allowed(
            &specified_origin,
            Some("https://allowed.example")
        ));
        assert!(!websocket_origin_allowed(
            &specified_origin,
            Some("https://blocked.example")
        ));
    }

    #[test]
    fn block_event_advances_subscription_cursor() {
        assert_eq!(block_range_after_event(10, 9, None), None);
        assert_eq!(block_range_after_event(10, 10, None), Some((10, 10, 11)));
        assert_eq!(block_range_after_event(10, 12, None), Some((10, 12, 13)));
        assert_eq!(
            block_range_after_event(u64::MAX, u64::MAX, None),
            Some((u64::MAX, u64::MAX, u64::MAX))
        );
    }

    #[test]
    fn block_event_respects_finite_to_block() {
        assert_eq!(
            block_range_after_event(10, 12, Some(11)),
            Some((10, 11, 12))
        );
        assert_eq!(block_range_after_event(12, 13, Some(11)), None);
        assert!(subscription_is_complete(12, Some(11)));
    }
}
