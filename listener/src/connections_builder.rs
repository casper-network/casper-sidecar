use anyhow::Error;
use async_trait::async_trait;
use casper_event_types::Filter;
use casper_types::ProtocolVersion;
use std::{collections::HashMap, net::IpAddr, sync::Arc, time::Duration};
use tokio::sync::{mpsc::Sender, Mutex};
use url::Url;

use crate::{
    connection_manager::{ConnectionManager, DefaultConnectionManagerBuilder},
    connection_tasks::ConnectionTasks,
    FilterWithEventId, SseEvent,
};

#[async_trait]
pub trait ConnectionsBuilder: Sync + Send {
    async fn build_connections(
        &self,
        last_event_id_for_filter: Arc<Mutex<HashMap<Filter, u32>>>,
        last_seen_event_id_sender: FilterWithEventId,
        node_build_version: ProtocolVersion,
    ) -> Result<HashMap<Filter, Box<dyn ConnectionManager>>, Error>;
}

pub struct DefaultConnectionsBuilder {
    pub sleep_between_keep_alive_checks: Duration,
    pub no_message_timeout: Duration,
    pub max_connection_attempts: usize,
    pub connection_timeout: Duration,
    pub sse_event_sender: Sender<SseEvent>,
    pub ip_address: IpAddr,
    pub sse_port: u16,
    pub allow_partial_connection: bool,
}

#[async_trait]
impl ConnectionsBuilder for DefaultConnectionsBuilder {
    async fn build_connections(
        &self,
        last_event_id_for_filter: Arc<Mutex<HashMap<Filter, u32>>>,
        last_seen_event_id_sender: FilterWithEventId,
        node_build_version: ProtocolVersion,
    ) -> Result<HashMap<Filter, Box<dyn ConnectionManager>>, Error> {
        let mut connections = HashMap::new();
        let filters = filters_from_version(node_build_version);
        let maybe_tasks =
            (!self.allow_partial_connection).then(|| ConnectionTasks::new(filters.len()));
        let guard = last_event_id_for_filter.lock().await;

        for filter in filters {
            let start_from_event_id = guard.get(&filter).copied().or(Some(0));
            let connection = self
                .build_connection(
                    maybe_tasks.clone(),
                    start_from_event_id,
                    filter.clone(),
                    last_seen_event_id_sender.clone(),
                )
                .await?;
            connections.insert(filter, connection);
        }
        drop(guard);
        Ok(connections)
    }
}

impl DefaultConnectionsBuilder {
    async fn build_connection(
        &self,
        maybe_tasks: Option<ConnectionTasks>,
        start_from_event_id: Option<u32>,
        filter: Filter,
        last_seen_event_id_sender: FilterWithEventId,
    ) -> Result<Box<dyn ConnectionManager>, Error> {
        let bind_address_for_filter = self.filtered_sse_url(&filter)?;
        let builder = DefaultConnectionManagerBuilder {
            bind_address: bind_address_for_filter,
            max_attempts: self.max_connection_attempts,
            sse_data_sender: self.sse_event_sender.clone(),
            maybe_tasks,
            connection_timeout: self.connection_timeout,
            start_from_event_id,
            filter,
            current_event_id_sender: last_seen_event_id_sender,
            sleep_between_keep_alive_checks: self.sleep_between_keep_alive_checks,
            no_message_timeout: self.no_message_timeout,
        };
        Ok(Box::new(builder.build()))
    }

    fn filtered_sse_url(&self, filter: &Filter) -> Result<Url, Error> {
        let url_str = format!("http://{}:{}/{}", self.ip_address, self.sse_port, filter);
        Url::parse(&url_str).map_err(Error::from)
    }
}

fn filters_from_version(_build_version: ProtocolVersion) -> Vec<Filter> {
    vec![Filter::Main, Filter::Sigs, Filter::Deploys]
}

pub struct ConnectionConfig {
    pub sleep_between_keep_alive_checks: Duration,
    pub no_message_timeout: Duration,
    pub max_connection_attempts: usize,
    pub connection_timeout: Duration,
    pub ip_address: IpAddr,
    pub sse_port: u16,
}

#[cfg(test)]
pub mod tests {
    use super::ConnectionsBuilder;
    use crate::{
        connection_manager::{tests::MockConnectionManager, ConnectionManager},
        FilterWithEventId,
    };
    use anyhow::Error;
    use async_trait::async_trait;
    use casper_event_types::Filter;
    use casper_types::ProtocolVersion;
    use std::{
        collections::{HashMap, HashSet},
        sync::Arc,
    };
    use tokio::sync::{
        mpsc::{channel, Receiver, Sender},
        Mutex,
    };

    pub type ResultsStoredInMock = Vec<Result<HashMap<Filter, Box<dyn ConnectionManager>>, Error>>;

    pub struct MockConnectionsBuilder {
        data_pushed_from_connections: Arc<Mutex<Vec<String>>>,
        result: Mutex<ResultsStoredInMock>,
        maybe_protocol_version: Mutex<Option<ProtocolVersion>>,
    }

    impl Default for MockConnectionsBuilder {
        fn default() -> Self {
            Self {
                data_pushed_from_connections: Arc::new(Mutex::new(vec![])),
                result: Mutex::new(vec![Ok(HashMap::new())]),
                maybe_protocol_version: Mutex::new(None),
            }
        }
    }

    impl MockConnectionsBuilder {
        pub fn one_fails_second_is_ok() -> Self {
            let (tx, rx) = channel(100);
            let results = vec![
                response_with_failing_events("1", &tx),
                response_with_all_connections_ok("2", &tx),
            ];
            Self::builder_based_on_result(rx, results)
        }
        pub fn one_ok() -> Self {
            let (tx, rx) = channel(100);
            let results = vec![response_with_all_connections_ok("1", &tx)];
            Self::builder_based_on_result(rx, results)
        }

        pub fn ok_after_two_fails() -> Self {
            let (tx, rx) = channel(100);
            let results = vec![
                Err(Error::msg("Connection failed")),
                Err(Error::msg("Connection failed 2")),
                response_with_all_connections_ok("2", &tx),
            ];
            Self::builder_based_on_result(rx, results)
        }

        pub fn connection_fails() -> Self {
            let (_, rx) = channel(100);
            let results = vec![Err(Error::msg("Connection failed"))];
            Self::builder_based_on_result(rx, results)
        }

        pub fn one_fails_immediatly() -> Self {
            let (tx, rx) = channel(100);
            let results = vec![response_with_failing_events("1", &tx)];
            Self::builder_based_on_result(rx, results)
        }

        pub async fn get_received_data(&self) -> HashSet<String> {
            let data = self.data_pushed_from_connections.lock().await;
            HashSet::from_iter(data.iter().cloned())
        }

        pub async fn get_recorded_protocol_version(&self) -> Option<ProtocolVersion> {
            let data = self.maybe_protocol_version.lock().await;
            *data
        }

        fn builder_based_on_result(mut rx: Receiver<String>, results: ResultsStoredInMock) -> Self {
            let data_pushed_from_connections = Arc::new(Mutex::new(vec![]));
            let data_pushed_from_connections_clone = data_pushed_from_connections.clone();
            tokio::spawn(async move {
                while let Some(x) = rx.recv().await {
                    let mut v = data_pushed_from_connections_clone.lock().await;
                    v.push(x);
                    drop(v);
                }
            });
            Self {
                data_pushed_from_connections,
                result: Mutex::new(results),
                maybe_protocol_version: Mutex::new(None),
            }
        }
    }

    fn response_with_all_connections_ok(
        msg_postfix: &str,
        tx: &Sender<String>,
    ) -> Result<HashMap<Filter, Box<dyn ConnectionManager>>, Error> {
        let events_msg = format!("events-{}", msg_postfix);
        let events: Box<dyn ConnectionManager> = Box::new(MockConnectionManager::ok_long(
            tx.clone(),
            Some(events_msg.as_str()),
        ));
        let main_msg = format!("main-{}", msg_postfix);
        let main: Box<dyn ConnectionManager> = Box::new(MockConnectionManager::ok_long(
            tx.clone(),
            Some(main_msg.as_str()),
        ));
        Ok(HashMap::from([
            (Filter::Events, events),
            (Filter::Main, main),
        ]))
    }

    fn response_with_failing_events(
        msg_postfix: &str,
        tx: &Sender<String>,
    ) -> Result<HashMap<Filter, Box<dyn ConnectionManager>>, Error> {
        let events: Box<dyn ConnectionManager> =
            Box::new(MockConnectionManager::fail_fast(tx.clone()));
        let main_msg = format!("main-{}", msg_postfix);
        let main: Box<dyn ConnectionManager> = Box::new(MockConnectionManager::ok_long(
            tx.clone(),
            Some(main_msg.as_str()),
        ));
        Ok(HashMap::from([
            (Filter::Events, events),
            (Filter::Main, main),
        ]))
    }

    #[async_trait]
    impl ConnectionsBuilder for MockConnectionsBuilder {
        async fn build_connections(
            &self,
            _last_event_id_for_filter: Arc<Mutex<HashMap<Filter, u32>>>,
            _last_seen_event_id_sender: FilterWithEventId,
            node_build_version: ProtocolVersion,
        ) -> Result<HashMap<Filter, Box<dyn ConnectionManager>>, Error> {
            let mut guard = self.maybe_protocol_version.lock().await;
            *guard = Some(node_build_version);
            drop(guard);
            let mut guard = self.result.lock().await;
            if !guard.is_empty() {
                return guard.remove(0);
            }
            Err(Error::msg("No more connections to build"))
        }
    }
}
