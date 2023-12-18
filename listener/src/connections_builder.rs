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
