#[cfg(test)]
pub(crate) mod tests {
    use async_stream::stream;
    use casper_event_types::sse_data::test_support::{
        example_block_added_1_4_10, example_finality_signature_1_4_10, BLOCK_HASH_1, BLOCK_HASH_2,
        BLOCK_HASH_3,
    };
    use casper_event_types::sse_data::SseData;
    use casper_event_types::sse_data_1_0_0::test_support::{example_block_added_1_0_0, shutdown};
    use casper_types::testing::TestRng;
    use futures::Stream;
    use hex_fmt::HexFmt;
    use std::convert::Infallible;
    use tokio::sync::broadcast::{
        channel as broadcast_channel, Receiver as BroadcastReceiver, Sender as BroadcastSender,
    };
    use tokio::sync::oneshot::{channel as oneshot_channel, Sender as OneshotSender};
    use warp::path::end;
    use warp::{sse::Event, Filter};
    use warp::{Rejection, Reply};

    fn build_stream(
        mut r: BroadcastReceiver<Option<(Option<String>, String)>>,
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

    pub async fn sse_server_example_data_1_0_0_two_blocks(port: u16) -> OneshotSender<()> {
        let data = vec![
            (None, "{\"ApiVersion\":\"1.0.0\"}".to_string()),
            (
                Some("0".to_string()),
                example_block_added_1_0_0(BLOCK_HASH_1, "1"),
            ),
            (
                Some("1".to_string()),
                example_block_added_1_0_0(BLOCK_HASH_3, "2"),
            ),
        ];
        simple_sse_server(port, data).await
    }

    pub async fn sse_server_shutdown_1_0_0(port: u16) -> OneshotSender<()> {
        let data = vec![
            (None, "{\"ApiVersion\":\"1.0.0\"}".to_string()),
            (Some("0".to_string()), shutdown()),
            (
                Some("1".to_string()),
                example_block_added_1_0_0(BLOCK_HASH_1, "1"),
            ),
        ];
        simple_sse_server(port, data).await
    }

    pub async fn sse_server_example_data_1_3_9_with_sigs(port: u16) -> OneshotSender<()> {
        let main_data = vec![
            (None, "{\"ApiVersion\":\"1.3.9\"}".to_string()),
            (
                Some("1".to_string()),
                example_block_added_1_4_10(BLOCK_HASH_2, "2"), //1.3.9 should use 1.4.x compliant BlockAdded messages
            ),
        ];
        let sigs_data = vec![
            (None, "{\"ApiVersion\":\"1.3.9\"}".to_string()),
            (
                Some("2".to_string()),
                example_finality_signature_1_4_10(BLOCK_HASH_2), //1.3.9 should use 1.4.x compliant FinalitySingatures messages
            ),
        ];
        simple_sse_server_with_sigs(port, main_data, sigs_data).await
    }

    pub async fn sse_server_send_n_block_added(
        port: u16,
        number_of_block_added_messages: u32,
        start_index: u32,
        rng: TestRng,
    ) -> (OneshotSender<()>, TestRng) {
        let (blocks_added, rng) =
            generate_random_blocks_added(number_of_block_added_messages, start_index, rng);

        let data = vec![(None, "{\"ApiVersion\":\"1.4.10\"}".to_string())];
        let data = data.into_iter().chain(blocks_added.into_iter()).collect();
        let sender = simple_sse_server(port, data).await;
        (sender, rng)
    }

    fn build_paths(
        main_data: Vec<(Option<String>, String)>,
    ) -> (
        Box<impl Filter<Extract = impl Reply, Error = Rejection> + Clone>,
        BroadcastSender<Option<(Option<String>, String)>>,
    ) {
        let (data_tx, _) = tokio::sync::broadcast::channel(100);
        let api = events_route(main_data.clone(), data_tx.clone())
            .or(main_events_route(main_data, data_tx.clone()));
        (Box::new(api), data_tx)
    }

    fn build_paths_with_sigs(
        main_data: Vec<(Option<String>, String)>,
        sigs_data: Vec<(Option<String>, String)>,
    ) -> (
        Box<impl Filter<Extract = impl Reply, Error = Rejection> + Clone>,
        BroadcastSender<Option<(Option<String>, String)>>,
        BroadcastSender<Option<(Option<String>, String)>>,
    ) {
        let (data_tx, _) = broadcast_channel(100);
        let (sigs_data_tx, _) = broadcast_channel(100);
        let api = events_route(main_data.clone(), data_tx.clone())
            .or(main_events_route(main_data, data_tx.clone()))
            .or(sigs_events_route(sigs_data, sigs_data_tx.clone()));
        (Box::new(api), data_tx, sigs_data_tx)
    }

    fn events_route(
        data: Vec<(Option<String>, String)>,
        sender: BroadcastSender<Option<(Option<String>, String)>>,
    ) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
        warp::path!("events")
            .and(warp::get())
            .map(move || {
                let local_data2 = data.clone();
                let reply = warp::sse::reply(
                    warp::sse::keep_alive().stream(build_stream(sender.clone().subscribe())),
                );
                local_data2.into_iter().for_each(|el| {
                    sender.send(Some(el)).unwrap();
                });
                reply
            })
            .and(end())
    }

    fn main_events_route(
        data: Vec<(Option<String>, String)>,
        sender: BroadcastSender<Option<(Option<String>, String)>>,
    ) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
        warp::path!("events" / "main")
            .and(warp::get())
            .map(move || {
                let local_data2 = data.clone();
                let reply = warp::sse::reply(
                    warp::sse::keep_alive().stream(build_stream(sender.clone().subscribe())),
                );
                local_data2.into_iter().for_each(|el| {
                    sender.send(Some(el)).unwrap();
                });
                reply
            })
            .and(end())
    }

    fn sigs_events_route(
        data: Vec<(Option<String>, String)>,
        sender: BroadcastSender<Option<(Option<String>, String)>>,
    ) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
        warp::path!("events" / "sigs")
            .and(warp::get())
            .map(move || {
                let local_data2 = data.clone();
                let reply = warp::sse::reply(
                    warp::sse::keep_alive().stream(build_stream(sender.clone().subscribe())),
                );
                local_data2.into_iter().for_each(|el| {
                    sender.send(Some(el)).unwrap();
                });
                reply
            })
            .and(end())
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

    pub async fn sse_server_example_data_1_1_0_with_legacy_message(port: u16) -> OneshotSender<()> {
        let data = vec![
            (None, "{\"ApiVersion\":\"1.1.0\"}".to_string()),
            (
                Some("1".to_string()),
                example_block_added_1_0_0(BLOCK_HASH_2, "3"),
            ),
        ];
        simple_sse_server(port, data).await
    }

    pub async fn simple_sse_server(
        port: u16,
        data: Vec<(Option<String>, String)>,
    ) -> OneshotSender<()> {
        let (shutdown_tx, shutdown_rx) = oneshot_channel();
        tokio::spawn(async move {
            let (api, main_data_tx) = build_paths(data);
            let server = warp::serve(*api)
                .bind_with_graceful_shutdown(([127, 0, 0, 1], port), async move {
                    shutdown_rx.await.ok();
                    let _ = main_data_tx.clone().send(None);
                })
                .1;
            server.await;
        });
        return shutdown_tx;
    }

    pub async fn simple_sse_server_with_sigs(
        port: u16,
        main_data: Vec<(Option<String>, String)>,
        sigs_data: Vec<(Option<String>, String)>,
    ) -> OneshotSender<()> {
        let (shutdown_tx, shutdown_rx) = oneshot_channel();
        tokio::spawn(async move {
            let (api, main_data_tx, sigs_data_tx) = build_paths_with_sigs(main_data, sigs_data);
            let server = warp::serve(*api)
                .bind_with_graceful_shutdown(([127, 0, 0, 1], port), async move {
                    shutdown_rx.await.ok();
                    let _ = main_data_tx.clone().send(None);
                    let _ = sigs_data_tx.clone().send(None);
                })
                .1;
            server.await;
        });
        return shutdown_tx;
    }

    fn generate_random_blocks_added(
        number_of_block_added_messages: u32,
        start_index: u32,
        mut rng: TestRng,
    ) -> (Vec<(Option<String>, String)>, TestRng) {
        let mut blocks_added = Vec::new();
        for i in 0..number_of_block_added_messages {
            let index = (i + start_index).to_string();
            let block_added = SseData::random_block_added(&mut rng);
            if let SseData::BlockAdded { block_hash, .. } = block_added {
                let encoded_hash = HexFmt(block_hash.inner()).to_string();
                let block_added_raw =
                    example_block_added_1_4_10(encoded_hash.as_str(), index.as_str());
                blocks_added.push((Some(index), block_added_raw));
            } else {
                panic!("random_block_added didn't return SseData::BlockAdded");
            }
        }
        (blocks_added, rng)
    }
}
