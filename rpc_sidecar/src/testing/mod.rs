use std::time::Duration;

use casper_binary_port::{
    BinaryMessage, BinaryMessageCodec, BinaryResponse, BinaryResponseAndRequest,
    GlobalStateQueryResult,
};
use casper_types::{bytesrepr::ToBytes, CLValue, ProtocolVersion, StoredValue};
use futures::{SinkExt, StreamExt};
use tokio::task::JoinHandle;
use tokio::{
    net::{TcpListener, TcpStream},
    time::sleep,
};
use tokio_util::codec::Framed;

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

    pub async fn start(&self) {
        let port = self.port;
        let addr = format!("{}:{}", LOCALHOST, port);
        let listener = TcpListener::bind(addr.clone())
            .await
            .expect("failed to listen");
        loop {
            match listener.accept().await {
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

pub async fn start_mock_binary_port_responding_with_stored_value(port: u16) -> JoinHandle<()> {
    let value = StoredValue::CLValue(CLValue::from_t("Foo").unwrap());
    let data = GlobalStateQueryResult::new(value, vec![]);
    let protocol_version = ProtocolVersion::from_parts(2, 0, 0);
    let val = BinaryResponse::from_value(data, protocol_version);
    let request = [];
    let response = BinaryResponseAndRequest::new(val, &request);
    start_mock_binary_port(port, response.to_bytes().unwrap()).await
}

async fn start_mock_binary_port(port: u16, data: Vec<u8>) -> JoinHandle<()> {
    let handler = tokio::spawn(async move {
        let binary_port = BinaryPortMock::new(port, data);
        binary_port.start().await;
    });
    sleep(Duration::from_secs(3)).await; // This should be handled differently, preferably the mock binary port should inform that it already bound to the port
    handler
}
