use std::sync::Arc;
use std::time::Duration;

use casper_binary_port::{
    BinaryMessage, BinaryMessageCodec, BinaryRequest, BinaryResponse, BinaryResponseAndRequest,
    GetRequest, GlobalStateQueryResult,
};
use casper_types::bytesrepr;
use casper_types::{bytesrepr::ToBytes, CLValue, ProtocolVersion, StoredValue};
use futures::{SinkExt, StreamExt};
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio::{
    net::{TcpListener, TcpStream},
    time::sleep,
};
use tokio_util::codec::Framed;

use crate::encode_request;

const LOCALHOST: &str = "127.0.0.1";
const MESSAGE_SIZE: u32 = 1024 * 1024 * 10;

pub struct BinaryPortMock {
    port: u16,
    response: Vec<u8>,
}

impl BinaryPortMock {
    pub fn new(port: u16, response: Vec<u8>) -> Self {
        Self { port, response }
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
                            tokio::spawn(handle_client(stream, response_payload));
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

async fn handle_client(stream: TcpStream, response: Vec<u8>) {
    let mut client = Framed::new(stream, BinaryMessageCodec::new(MESSAGE_SIZE));

    let next_message = client.next().await;
    if next_message.is_some() {
        tokio::spawn({
            async move {
                let _ = client.send(BinaryMessage::new(response)).await;
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
    shutdown: Arc<Notify>,
) -> JoinHandle<()> {
    let value = StoredValue::CLValue(CLValue::from_t("Foo").unwrap());
    let data = GlobalStateQueryResult::new(value, vec![]);
    let protocol_version = ProtocolVersion::from_parts(2, 0, 0);
    let val = BinaryResponse::from_value(data, protocol_version);
    let request = get_dummy_request_payload(request_id);
    let response = BinaryResponseAndRequest::new(val, &request, request_id.unwrap_or_default());
    start_mock_binary_port(port, response.to_bytes().unwrap(), shutdown).await
}

pub async fn start_mock_binary_port(
    port: u16,
    data: Vec<u8>,
    shutdown: Arc<Notify>,
) -> JoinHandle<()> {
    let handler = tokio::spawn(async move {
        let binary_port = BinaryPortMock::new(port, data);
        binary_port.start(shutdown).await;
    });
    sleep(Duration::from_secs(3)).await; // This should be handled differently, preferably the mock binary port should inform that it already bound to the port
    handler
}

pub(crate) fn get_dummy_request() -> BinaryRequest {
    BinaryRequest::Get(GetRequest::Information {
        info_type_tag: 0,
        key: vec![],
    })
}

pub(crate) fn get_dummy_request_payload(request_id: Option<u16>) -> bytesrepr::Bytes {
    let dummy_request = get_dummy_request();
    encode_request(&dummy_request, request_id.unwrap_or_default())
        .unwrap()
        .into()
}
