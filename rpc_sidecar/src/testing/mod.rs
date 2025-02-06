use std::sync::Arc;
use std::time::Duration;

use casper_binary_port::{
    BinaryMessage, BinaryMessageCodec, BinaryResponse, BinaryResponseAndRequest, Command,
    GetRequest, GlobalStateQueryResult,
};
use casper_types::bytesrepr::Bytes;
use casper_types::{bytesrepr::ToBytes, CLValue, StoredValue};
use futures::{SinkExt, StreamExt};
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio::{
    net::{TcpListener, TcpStream},
    time::sleep,
};
use tokio_util::codec::{Encoder, Framed};

use crate::encode_request;

const LOCALHOST: &str = "127.0.0.1";
const MESSAGE_SIZE: u32 = 1024 * 1024 * 10;

pub struct BinaryPortMock {
    port: u16,
    response: Vec<u8>,
    number_of_responses: u8,
}

impl BinaryPortMock {
    pub fn new(port: u16, response: Vec<u8>, number_of_responses: u8) -> Self {
        Self {
            port,
            response,
            number_of_responses,
        }
    }

    pub async fn start(&self, shutdown: Arc<Notify>) {
        let port = self.port;
        let addr = format!("{}:{}", LOCALHOST, port);
        let listener = TcpListener::bind(addr.clone())
            .await
            .expect("failed to listen");
        loop {
            tokio::select! {
                _ = shutdown.notified() => {
                    break;
                }
                val = listener.accept() => {
                    match val {
                        Ok((stream, _addr)) => {
                            let response_payload = self.response.clone();
                            tokio::spawn(handle_client(stream, response_payload, self.number_of_responses));
                        }
                        Err(io_err) => {
                            println!("acceptance failure: {:?}", io_err);
                        }
                    }
                }
            }
        }
    }
}

async fn handle_client(stream: TcpStream, response: Vec<u8>, number_of_responses: u8) {
    let mut client = Framed::new(stream, BinaryMessageCodec::new(MESSAGE_SIZE));

    let next_message = client.next().await;
    if next_message.is_some() {
        tokio::spawn({
            async move {
                for _ in 0..number_of_responses {
                    let _ = client.send(BinaryMessage::new(response.clone())).await;
                }
            }
        });
    }
}

pub fn get_port() -> u16 {
    portpicker::pick_unused_port().unwrap()
}

pub async fn start_mock_binary_port_responding_with_stored_value(
    port: u16,
    request_id: Option<u16>,
    number_of_responses: Option<u8>,
    shutdown: Arc<Notify>,
) -> JoinHandle<()> {
    let value = StoredValue::CLValue(CLValue::from_t("Foo").unwrap());
    let data = GlobalStateQueryResult::new(value, vec![]);
    let val = BinaryResponse::from_value(data);
    let request = get_dummy_request_payload(request_id);
    let response = BinaryResponseAndRequest::new(val, request);
    start_mock_binary_port(
        port,
        response.to_bytes().unwrap(),
        number_of_responses.unwrap_or(1), // Single response by default
        shutdown,
    )
    .await
}

pub async fn start_mock_binary_port_responding_with_given_response(
    port: u16,
    request_id: Option<u16>,
    number_of_responses: Option<u8>,
    shutdown: Arc<Notify>,
    binary_response: BinaryResponse,
) -> JoinHandle<()> {
    let request = get_dummy_request_payload(request_id);
    let response = BinaryResponseAndRequest::new(binary_response, request);
    start_mock_binary_port(
        port,
        response.to_bytes().unwrap(),
        number_of_responses.unwrap_or(1), // Single response by default
        shutdown,
    )
    .await
}

pub async fn start_mock_binary_port(
    port: u16,
    data: Vec<u8>,
    number_of_responses: u8,
    shutdown: Arc<Notify>,
) -> JoinHandle<()> {
    let handler = tokio::spawn(async move {
        let binary_port = BinaryPortMock::new(port, data, number_of_responses);
        binary_port.start(shutdown).await;
    });
    sleep(Duration::from_secs(3)).await; // This should be handled differently, preferably the mock binary port should inform that it already bound to the port
    handler
}

pub(crate) fn get_dummy_request() -> Command {
    Command::Get(GetRequest::Information {
        info_type_tag: 0,
        key: vec![],
    })
}

pub(crate) fn get_dummy_request_payload(request_id: Option<u16>) -> Bytes {
    let dummy_request = get_dummy_request();
    let bytes = encode_request(&dummy_request, request_id.unwrap_or_default()).unwrap();
    let mut codec = BinaryMessageCodec::new(u32::MAX);
    let mut buf = bytes::BytesMut::new();
    codec.encode(BinaryMessage::new(bytes), &mut buf).unwrap();
    Bytes::from(buf.freeze().to_vec())
}
