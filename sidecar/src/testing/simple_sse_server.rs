#[cfg(test)]
pub(crate) mod tests {
    use async_stream::stream;
    use casper_event_types::sse_data::tests::{
        example_block_added_1_4_10, BLOCK_HASH_1, BLOCK_HASH_2,
    };
    use casper_event_types::sse_data_1_0_0::tests::example_block_added_1_0_0;
    use futures::Stream;
    use std::convert::Infallible;
    use tokio::sync::broadcast::*;
    use tokio::sync::oneshot::{channel as oneshot_channel, Sender as OneshotSender};
    use warp::path::end;
    use warp::{sse::Event, Filter};

    fn build_stream(
        mut r: Receiver<Option<(Option<String>, String)>>,
    ) -> impl Stream<Item = Result<Event, Infallible>> {
        stream! {
            while let Ok(z) = r.recv().await {
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

    pub async fn sse_server_example_data_1_0_0(port: u16) -> OneshotSender<()> {
        let data = vec![
            (None, "{\"ApiVersion\":\"1.0.0\"}".to_string()),
            (
                Some("0".to_string()),
                example_block_added_1_0_0(BLOCK_HASH_1, "1"),
            ),
        ];
        simple_sse_server(port, data).await
    }

    pub async fn sse_server_example_data_1_4_10(port: u16) -> OneshotSender<()> {
        let data = vec![
            (None, "{\"ApiVersion\":\"1.4.10\"}".to_string()),
            (
                Some("1".to_string()),
                example_block_added_1_4_10(BLOCK_HASH_2, "2"),
            ),
        ];
        simple_sse_server(port, data).await
    }

    pub async fn simple_sse_server(
        port: u16,
        data: Vec<(Option<String>, String)>,
    ) -> OneshotSender<()> {
        let data2 = data.clone();
        let (shutdown_tx, shutdown_rx) = oneshot_channel();
        let (data_tx, _) = tokio::sync::broadcast::channel(100);
        let data_tx2 = data_tx.clone();
        tokio::spawn(async move {
            let data_tx3 = data_tx2.clone();
            let data_tx4 = data_tx3.clone();

            let main = warp::path("main");
            let main = main.and(end()).map(move || {
                let local_data2 = data.clone();
                let reply = warp::sse::reply(
                    warp::sse::keep_alive().stream(build_stream(data_tx4.subscribe())),
                );
                local_data2.into_iter().for_each(|el| {
                    data_tx4.send(Some(el)).unwrap();
                });
                reply
            });
            let events = warp::path("events");
            let events = events
                .and(end())
                .map(move || {
                    let local_data2 = data2.clone();
                    let reply = warp::sse::reply(
                        warp::sse::keep_alive().stream(build_stream(data_tx3.subscribe())),
                    );
                    local_data2.into_iter().for_each(|el| {
                        data_tx3.send(Some(el)).unwrap();
                    });
                    reply
                })
                .or(events.and(main));

            let server = warp::serve(warp::get().and(events))
                .bind_with_graceful_shutdown(([127, 0, 0, 1], port), async move {
                    shutdown_rx.await.ok();
                    let _ = data_tx.send(None);
                })
                .1;
            server.await;
        });
        return shutdown_tx;
    }
}
