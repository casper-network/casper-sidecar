#[cfg(test)]
pub(crate) mod tests {
    use async_stream::stream;
    use futures::Stream;
    use std::collections::HashMap;
    use std::convert::Infallible;
    use std::iter::FromIterator;
    use std::sync::Arc;
    use tokio::sync::broadcast::{self};
    use tokio::sync::mpsc::{channel, Receiver, Sender};
    use tokio::sync::Mutex;
    use warp::filters::BoxedFilter;
    use warp::{path, sse::Event, Filter, Rejection, Reply};

    use crate::testing::raw_sse_events_utils::tests::EventsWithIds;

    type BroadcastSender = broadcast::Sender<Option<(Option<String>, String)>>;
    type BroadcastReceiver = broadcast::Receiver<Option<(Option<String>, String)>>;

    #[derive(Clone)]
    pub struct CacheAndData {
        //cache and data are vectors of tuples -> first position is event id, second is raw payload of message
        pub cache: EventsWithIds,
        pub data: EventsWithIds,
    }

    pub(crate) struct SimpleSseServer {
        pub routes: HashMap<Vec<String>, CacheAndData>,
    }

    type ShutdownCallbacks = Arc<Mutex<Vec<broadcast::Sender<Option<(Option<String>, String)>>>>>;

    impl SimpleSseServer {
        pub async fn serve(&self, port: u16) -> (Sender<()>, Receiver<()>) {
            let (shutdown_tx, mut shutdown_rx) = channel(10);
            let (after_shutdown_tx, after_shutdown_rx) = channel(10);
            let shutdown_broadcasts: Arc<Mutex<Vec<BroadcastSender>>> =
                Arc::new(Mutex::new(Vec::new()));
            let mut routes = self.build_filter(shutdown_broadcasts.clone());
            let first = routes.pop().expect("get first route");
            let api = routes
                .into_iter()
                .fold(first, |e, r| e.or(r).unify().boxed());
            let server_thread = tokio::spawn(async move {
                let server = warp::serve(api)
                    .bind_with_graceful_shutdown(([127, 0, 0, 1], port), async move {
                        let _ = shutdown_rx.recv().await;
                        let guard = shutdown_broadcasts.lock().await;
                        for sender in guard.iter() {
                            let _ = sender.send(None);
                        }
                        drop(guard);
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
            (shutdown_tx, after_shutdown_rx)
        }

        fn build_filter(
            &self,
            shutdown_broadcasts: ShutdownCallbacks,
        ) -> Vec<BoxedFilter<(Box<dyn Reply>,)>> {
            let routes: Vec<BoxedFilter<(Box<dyn Reply>,)>> = Vec::from_iter(self.routes.iter())
                .into_iter()
                .map(|(key, value)| {
                    let base_filter = key
                        .iter()
                        .fold(warp::get().boxed(), |route, part| {
                            route.and(warp::path(part.clone())).boxed()
                        })
                        .and(path::end())
                        .boxed();
                    let data = value.clone();
                    base_filter
                        .and(with_data(data))
                        .and(warp::query())
                        .and(with_shutdown_broadcasts(shutdown_broadcasts.clone()))
                        .and_then(
                            |data,
                             query,
                             shutdown_broadcasts: Arc<Mutex<Vec<BroadcastSender>>>| async move {
                                let (sender, _) = tokio::sync::broadcast::channel(100);
                                let mut guard = shutdown_broadcasts.lock().await;
                                guard.push(sender.clone());
                                drop(guard);
                                do_handle(data, sender.clone(), query)
                                    .await
                            },
                        )
                        .boxed()
                })
                .collect();
            routes
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
            .and_then(|id_str| id_str.parse::<u32>().ok());
        let reply =
            warp::sse::reply(warp::sse::keep_alive().stream(build_stream(sender.subscribe())));
        let mut effective_data = Vec::new();
        if let Some(start_from) = maybe_start_from {
            let mut filtered: EventsWithIds = cache_and_data
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

    fn with_shutdown_broadcasts(
        x: Arc<Mutex<Vec<BroadcastSender>>>,
    ) -> impl Filter<Extract = (Arc<Mutex<Vec<BroadcastSender>>>,), Error = Infallible> + Clone
    {
        warp::any().map(move || x.clone())
    }
}
