mod utils;

use std::{
    collections::BTreeMap,
    fmt::{Debug, Display, Formatter},
    time::Duration,
};

use anyhow::Error;
use bytes::Bytes;
use eventsource_stream::{EventStream, Eventsource};
use futures::StreamExt;
use reqwest::Client;
use tokio::sync::{
    mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
    oneshot::{channel as oneshot_channel, Receiver as OneshotReceiver, Sender as OneshotSender},
};
use tokio_stream::Stream;
use tracing::{error, info, warn};

use casper_event_types::SseData;
use casper_types::ProtocolVersion;
use serde::Deserialize;

use crate::utils::resolve_address;

#[derive(Clone)]
pub enum SseFilter {
    Main,
    Deploys,
    Sigs,
}

impl Display for SseFilter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            SseFilter::Main => write!(f, "main"),
            SseFilter::Deploys => write!(f, "deploys"),
            SseFilter::Sigs => write!(f, "sigs"),
        }
    }
}

#[derive(Clone, Deserialize)]
pub struct FilterPriority {
    main: u8,
    sigs: u8,
    deploys: u8,
}

impl FilterPriority {
    pub fn new(main: u8, sigs: u8, deploys: u8) -> Result<Self, Error> {
        if main != sigs && main != deploys && sigs != deploys {
            Ok(Self {
                main,
                sigs,
                deploys,
            })
        } else {
            Err(Error::msg(
                "Priorities cannot be repeated, each filter must be assigned a different value",
            ))
        }
    }
}

impl Default for FilterPriority {
    fn default() -> Self {
        Self {
            main: 0,
            sigs: 1,
            deploys: 2,
        }
    }
}

#[derive(Clone)]
pub struct EventListener {
    bind_address: String,
    pub api_version: ProtocolVersion,
    current_event_id: Option<u32>,
    max_retries: u8,
    delay_between_retries: u8,
    allow_partial_connection: bool,
    filter_priority: FilterPriority,
}

pub struct SseEvent {
    pub source: String,
    pub id: Option<u32>,
    pub data: SseData,
}

/// Pass the [`waiter_rx`] to the connecting task.
/// Use the [`reporter_tx`] as the payload for the `waiter` channel.  
/// [`connect_with_retry`] will await receipt of the sender  from the [`waiter_tx`].
/// Once it has connected successfully it will report it by responding along the `reporter` channel.  
/// Finally the managing task/function will have kept the [`reporter_rx`] to know when the connection is live.
struct ConnectionWaiterReporter {
    waiter_tx: OneshotSender<OneshotSender<()>>,
    reporter_tx: OneshotSender<()>,
    reporter_rx: OneshotReceiver<()>,
}

impl ConnectionWaiterReporter {
    fn new() -> (Self, OneshotReceiver<OneshotSender<()>>) {
        let (waiter_tx, waiter_rx) = oneshot_channel::<OneshotSender<()>>();
        let (reporter_tx, reporter_rx) = oneshot_channel::<()>();

        (
            Self {
                waiter_tx,
                reporter_tx,
                reporter_rx,
            },
            waiter_rx,
        )
    }

    async fn connect(self) -> bool {
        let _ = self.waiter_tx.send(self.reporter_tx);
        self.reporter_rx.await.is_ok()
    }
}

impl EventListener {
    /// Returns an instance of [EventListener] based on the given `bind_address`.  
    /// `delay_between_retries` should be provided in seconds.
    ///
    /// Will error if:
    /// - `bind_address` cannot be parsed into a network address.
    /// - It is unable to connect to the source.
    /// - The source doesn't send a valid [ApiVersion] as the first event.
    pub async fn new(
        bind_address: String,
        max_retries: u8,
        delay_between_retries: u8,
        allow_partial_connection: bool,
        filter_priority: FilterPriority,
    ) -> Result<Self, Error> {
        resolve_address(&bind_address)?;
        let prefixed_address = format!("http://{}", bind_address);

        let address_with_filter = format!("{}/events/main", prefixed_address);
        let stream = Client::new()
            .get(&address_with_filter)
            .send()
            .await?
            .bytes_stream()
            .eventsource();

        let (api_version, _) = parse_api_version(stream).await?;

        Ok(Self {
            bind_address: prefixed_address,
            api_version,
            current_event_id: None,
            max_retries,
            delay_between_retries,
            allow_partial_connection,
            filter_priority,
        })
    }

    pub async fn consume_combine_streams(&self) -> Result<UnboundedReceiver<SseEvent>, Error> {
        let (sse_tx, sse_rx) = unbounded_channel::<SseEvent>();

        let mut connection_handlers = BTreeMap::new();

        let (main_connection_handler, main_waiter_rx) = ConnectionWaiterReporter::new();
        assert!(
            connection_handlers
                .insert(self.filter_priority.main, main_connection_handler)
                .is_none(),
            "Filters must have distinct values for priority"
        );

        let main_filter_handle = tokio::spawn(connect_with_retry(
            sse_tx.clone(),
            self.bind_address.clone(),
            SseFilter::Main,
            self.current_event_id,
            self.max_retries,
            self.delay_between_retries,
            main_waiter_rx,
        ));

        let (deploys_connection_handler, deploys_waiter_rx) = ConnectionWaiterReporter::new();
        assert!(
            connection_handlers
                .insert(self.filter_priority.deploys, deploys_connection_handler)
                .is_none(),
            "Filters must have distinct values for priority"
        );

        let deploys_filter_handle = tokio::spawn(connect_with_retry(
            sse_tx.clone(),
            self.bind_address.clone(),
            SseFilter::Deploys,
            self.current_event_id,
            self.max_retries,
            self.delay_between_retries,
            deploys_waiter_rx,
        ));

        let (sigs_connection_handler, sigs_waiter_rx) = ConnectionWaiterReporter::new();
        assert!(
            connection_handlers
                .insert(self.filter_priority.sigs, sigs_connection_handler)
                .is_none(),
            "Filters must have distinct values for priority"
        );

        let sigs_filter_handle = tokio::spawn(connect_with_retry(
            sse_tx.clone(),
            self.bind_address.clone(),
            SseFilter::Sigs,
            self.current_event_id,
            self.max_retries,
            self.delay_between_retries,
            sigs_waiter_rx,
        ));

        let allow_partial_connection = self.allow_partial_connection;
        let bind_address = self.bind_address.clone();
        tokio::spawn(async move {
            if allow_partial_connection {
                let _ = tokio::join!(
                    main_filter_handle,
                    deploys_filter_handle,
                    sigs_filter_handle
                );
                warn!("All filters have disconnected");
            } else {
                tokio::select! {
                    result = main_filter_handle => {
                       warn!(?result, "Failed to connect to MAIN filter - allow_partial_connection is set to false - disconnecting from node")
                    },
                    result = deploys_filter_handle => {
                       warn!(?result, "Failed to connect to DEPLOYS filter - allow_partial_connection is set to false - disconnecting from node")
                    },
                    result = sigs_filter_handle => {
                       warn!(?result, "Failed to connect to SIGS filter - allow_partial_connection is set to false - disconnecting from node")
                    }
                }
            }
            // Retries have been exhausted without a successful connection - send Shutdown.
            let _ = sse_tx.send(SseEvent {
                source: bind_address,
                id: None,
                data: SseData::Shutdown,
            });
        });

        let results_future = tokio::spawn(async move {
            let mut connected_to_all = true;
            for (_priority, handler) in connection_handlers {
                connected_to_all = handler.connect().await && connected_to_all;
            }
            connected_to_all
        });

        // If partial connection is acceptable then we can return the receiver immediately.
        if allow_partial_connection {
            Ok(sse_rx)
        } else {
            // Otherwise, we need to wait for all filters/connections to report
            let connected_to_all = results_future.await.expect("should run connection task");
            // If all filters are not connected then we need to return an Err since partial connection is not allowed
            if !connected_to_all {
                return Err(Error::msg("Partial connection is disabled, the listener was unable to connect all filters"));
            }
            Ok(sse_rx)
        }
    }
}

async fn connect_with_retry(
    event_sender: UnboundedSender<SseEvent>,
    bind_address: String,
    filter: SseFilter,
    start_from_id: Option<u32>,
    max_retries: u8,
    delay_between: u8,
    wait_turn_then_report: OneshotReceiver<OneshotSender<()>>,
) {
    let mut retry_count = 0;
    let mut current_event_id = start_from_id;

    // Wait until this filter has been given it's turn to attempt connection
    let connected_reporter = wait_turn_then_report
        .await
        .expect("Connection reporting channel failed - cannot control filter priority without it");

    let mut exhaustible_connected_reporter = Some(connected_reporter);

    while retry_count <= max_retries {
        if retry_count > 0 {
            info!(
                bind_address,
                %filter, "Attempting to reconnect... ({}/{})", retry_count, max_retries
            );
        }

        let mut url = format!("{}/events/{}", bind_address, filter);
        if let Some(event_id) = current_event_id {
            let start_from_query = format!("?start_from={}", event_id);
            url.push_str(&start_from_query);
        }

        let (api_version, mut event_stream) = match connect(url).await {
            Ok(ok_val) => {
                // reset retry counter since the connection was successful
                retry_count = 0;
                ok_val
            }
            Err(_) => {
                // increment retry counter since the connection failed
                retry_count += 1;
                if retry_count > max_retries {
                    error!(
                        %bind_address,
                        %filter,
                        "Error connecting, retries exhausted"
                    );
                } else {
                    error!(
                        %bind_address,
                        %filter,
                        "Error connecting, retrying in {}s",
                        delay_between
                    );
                }

                // wait the configured delay before continuing
                tokio::time::sleep(Duration::from_secs(delay_between as u64)).await;
                continue;
            }
        };

        if let Some(reporter) = exhaustible_connected_reporter.take() {
            let _ = reporter.send(());
        }

        info!(%bind_address, %filter, %api_version, "Connected to event stream{}", {
            if let Some(event_id) = start_from_id {
                format!(", resuming from id: {}", event_id)
            } else {
                "".to_string()
            }
        });

        while let Some(event) = event_stream.next().await {
            match event {
                Ok(event) => {
                    let event_id: u32 = event.id.parse().expect("Error parsing id into u32");
                    current_event_id = Some(event_id);

                    let cloned_sender = event_sender.clone();

                    match serde_json::from_str::<SseData>(&event.data) {
                        Ok(SseData::Shutdown) | Err(_) => {
                            error!(
                                %bind_address,
                                %filter,
                                "Error connecting, retrying in {}s",
                                delay_between
                            );
                            retry_count += 1;
                            tokio::time::sleep(Duration::from_secs(delay_between as u64)).await;
                            break;
                        }
                        Ok(sse_data) => {
                            let _ = cloned_sender.send(SseEvent {
                                source: bind_address.clone(),
                                id: Some(event_id),
                                data: sse_data,
                            });
                        }
                    }
                }
                Err(event_stream_err) => {
                    warn!(%bind_address, %filter, message = "Error returned from event stream", error=?event_stream_err)
                }
            }
        }
    }
    warn!(%bind_address, %filter, "Error connecting to node - if allow_partial_connection = true, listener will stay connected on other filters");
}

async fn connect(
    url: String,
) -> Result<
    (
        ProtocolVersion,
        EventStream<impl Stream<Item = reqwest::Result<Bytes>> + Sized>,
    ),
    Error,
> {
    let event_stream = Client::new()
        .get(&url)
        .send()
        .await?
        .bytes_stream()
        .eventsource();

    // Parse the first event to see if the connection was successful
    let (api_version, event_stream) = parse_api_version(event_stream).await?;

    Ok((api_version, event_stream))
}

async fn parse_api_version<S, E>(
    mut stream: EventStream<S>,
) -> Result<(ProtocolVersion, EventStream<S>), Error>
where
    E: Debug,
    S: Stream<Item = Result<Bytes, E>> + Sized + Unpin,
{
    match stream.next().await {
        None => Err(Error::msg("First event was empty")),
        Some(Err(error)) => Err(Error::msg(format!(
            "failed to get first event: {:?}",
            error
        ))),
        Some(Ok(event)) => match serde_json::from_str::<SseData>(&event.data) {
            Ok(SseData::ApiVersion(api_version)) => Ok((api_version, stream)),
            Ok(_) => Err(Error::msg("First event should have been API version")),
            Err(serde_err) => Err(Error::from(serde_err)
                .context("First event could not be deserialized into ApiVersion")),
        },
    }
}
