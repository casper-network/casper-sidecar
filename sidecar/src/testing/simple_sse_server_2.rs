#[cfg(test)]
pub(crate) mod tests {
    use async_stream::stream;
    use casper_event_types::{
        sse_data::test_support::{BLOCK_HASH_1, BLOCK_HASH_2},
        sse_data_1_0_0::test_support::example_block_added_1_0_0,
    };
    use futures::Stream;
    use std::collections::HashMap;
    use std::convert::Infallible;
    use std::iter::FromIterator;
    use tokio::sync::broadcast::{self};
    use tokio::sync::mpsc::{channel, Receiver, Sender};
    use warp::filters::BoxedFilter;
    use warp::{sse::Event, Filter, Rejection, Reply};

    use crate::testing::fake_event_stream::wait_for_sse_server_to_be_up;

    type BroadcastSender = broadcast::Sender<Option<(Option<String>, String)>>;
    type BroadcastReceiver = broadcast::Receiver<Option<(Option<String>, String)>>;

    #[derive(Clone)]
    pub struct CacheAndData {
        //cache and data are vectors of tuples -> first position is event id, second is raw payload of message
        cache: Vec<(Option<String>, String)>,
        data: Vec<(Option<String>, String)>,
    }

    pub(crate) struct SimpleSseServer {
        routes: HashMap<Vec<String>, CacheAndData>,
    }

    #[derive(Debug)]
    struct Nope;
    impl warp::reject::Reject for Nope {}

    impl SimpleSseServer {
        pub async fn serve(&self, port: u16) -> (Sender<()>, Receiver<()>) {
            let (shutdown_tx, mut shutdown_rx) = channel(10);
            let (after_shutdown_tx, after_shutdown_rx) = channel(10);
            let (sender, _) = tokio::sync::broadcast::channel(100);
            let v = Vec::from_iter(self.routes.iter());
            let mut shutdown_broadcasts: Vec<BroadcastSender> = Vec::new();
            let mut routes: Vec<BoxedFilter<(Box<dyn Reply>,)>> = v
                .into_iter()
                .map(|(key, value)| {
                    let base_filter = key.into_iter().fold(warp::get().boxed(), |route, part| {
                        route.and(warp::path(part.clone())).boxed()
                    });
                    let data = value.clone();
                    shutdown_broadcasts.add(sender.clone());
                    base_filter
                        .and(with_data(data))
                        .and(with_sender(sender.clone()))
                        .and(warp::query())
                        .and_then(do_handle)
                        .boxed()
                })
                .collect();
            let first = routes.pop().expect("get first route");
            let api = routes
                .into_iter()
                .fold(first, |e, r| e.or(r).unify().boxed());
            let server_thread = tokio::spawn(async move {
                let server = warp::serve(api)
                    .bind_with_graceful_shutdown(([127, 0, 0, 1], port), async move {
                        shutdown_rx.recv().await.unwrap();
                        for sender in shutdown_broadcasts.iter() {
                            let _ = sender.send(None);
                        }
                    })
                    .1;
                server.await;
                let _ = after_shutdown_tx.send(()).await;
            });
            tokio::spawn(async move {
                let result = server_thread.await;
                if result.is_err() {
                    println!("simple_sse_server: {:?}", result);
                }
            });
            let urls: Vec<String> = vec![format!("http://127.0.0.1:{}/events/main", port)];
            wait_for_sse_server_to_be_up(urls).await;
            return (shutdown_tx, after_shutdown_rx);
        }
    }

    fn build_stream(
        mut broadcast_receiver: BroadcastReceiver,
    ) -> impl Stream<Item = Result<Event, Infallible>> {
        stream! {
            while let Ok(z) = broadcast_receiver.recv().await {
                match z {
                    Some(event_data) => {
                        let mut event = Event::default().data(event_data.1);
                        if let Some(id) = event_data.0 {
                            event = event.id(id);
                        }
                        yield Ok(event);
                    },
                    None => break,
                }
            }
        }
    }

    async fn do_handle(
        mut cache_and_data: CacheAndData,
        sender: BroadcastSender,
        query: HashMap<String, String>,
    ) -> Result<Box<dyn Reply>, Rejection> {
        let maybe_start_from = query
            .get("start_from")
            .and_then(|start_from_raw| start_from_raw.parse::<u32>().ok());
        let reply = warp::sse::reply(
            warp::sse::keep_alive().stream(build_stream(sender.clone().subscribe())),
        );
        let mut effective_data = Vec::new();
        if let Some(start_from) = maybe_start_from {
            let mut filtered: Vec<(Option<String>, String)> = cache_and_data
                .cache
                .into_iter()
                .filter(|(raw_maybe_id, _)| {
                    let maybe_id = raw_maybe_id.clone().and_then(|x| x.parse::<u32>().ok());
                    match maybe_id {
                        None => true,
                        Some(id) if (id >= start_from) => true,
                        _ => false,
                    }
                })
                .collect();
            effective_data.append(filtered.as_mut());
        }
        effective_data.append(cache_and_data.data.as_mut());
        effective_data.into_iter().for_each(|el| {
            sender.send(Some(el)).unwrap();
        });
        Ok(Box::new(reply) as Box<dyn Reply>)
    }

    fn with_data(
        x: CacheAndData,
    ) -> impl Filter<Extract = (CacheAndData,), Error = Infallible> + Clone {
        warp::any().map(move || x.clone())
    }

    fn with_sender(
        sender: BroadcastSender,
    ) -> impl Filter<Extract = (BroadcastSender,), Error = Infallible> + Clone {
        warp::any().map(move || sender.clone())
    }

    fn parse_query(query: HashMap<String, String>) -> Option<u32> {
        match query
            .get("start_from")
            .and_then(|id_str| id_str.parse::<u32>().ok())
        {
            Some(id) => Some(id),
            None => None,
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn sidecar_should_use_start_from_if_database_is_empty() {
        let cache = vec![
            (None, "{\"ApiVersion\":\"1.1.0\"}".to_string()),
            (
                Some("0".to_string()),
                example_block_added_1_0_0(BLOCK_HASH_2, "3"),
            ),
        ];
        let data = vec![
            (None, "{\"ApiVersion\":\"1.1.0\"}".to_string()),
            (
                Some("1".to_string()),
                example_block_added_1_0_0(BLOCK_HASH_1, "3"),
            ),
        ];
        let cache_and_data = CacheAndData { cache, data };
        let a = SimpleSseServer {
            routes: HashMap::from([(vec!["a".to_string(), "b".to_string()], cache_and_data)]),
        };

        let join = tokio::spawn(async move { a.serve(3333).await });
        let results = join.await;
    }
}
