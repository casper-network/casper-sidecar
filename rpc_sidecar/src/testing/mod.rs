use bytes::{BufMut, BytesMut};
use juliet::{
    io::IoCoreBuilder,
    protocol::ProtocolBuilder,
    rpc::{IncomingRequest, RpcBuilder},
    ChannelConfiguration, ChannelId,
};
use tokio::net::{TcpListener, TcpStream};

const LOCALHOST: &str = "127.0.0.1";

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
        let protocol_builder = ProtocolBuilder::<1>::with_default_channel_config(
            ChannelConfiguration::default()
                .with_request_limit(300)
                .with_max_request_payload_size(1000)
                .with_max_response_payload_size(1000),
        );

        let io_builder = IoCoreBuilder::new(protocol_builder).buffer_size(ChannelId::new(0), 20);

        let rpc_builder = Box::leak(Box::new(RpcBuilder::new(io_builder)));
        let listener = TcpListener::bind(addr.clone())
            .await
            .expect("failed to listen");
        loop {
            match listener.accept().await {
                Ok((client, _addr)) => {
                    let response_payload = self.response.clone();
                    tokio::spawn(handle_client(client, rpc_builder, response_payload));
                }
                Err(io_err) => {
                    println!("acceptance failure: {:?}", io_err);
                }
            }
        }
    }
}

async fn handle_client<const N: usize>(
    mut client: TcpStream,
    rpc_builder: &RpcBuilder<N>,
    response: Vec<u8>,
) {
    let (reader, writer) = client.split();
    let (client, mut server) = rpc_builder.build(reader, writer);
    while let Ok(Some(incoming_request)) = server.next_request().await {
        tokio::spawn(handle_request(incoming_request, response.clone()));
    }
    drop(client);
}

async fn handle_request(incoming_request: IncomingRequest, response: Vec<u8>) {
    let mut response_payload = BytesMut::new();
    let byt = response;
    for b in byt {
        response_payload.put_u8(b);
    }
    incoming_request.respond(Some(response_payload.freeze()));
}
