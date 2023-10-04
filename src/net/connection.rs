use std::io::Error;

use log::debug;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::net::endpoint::Endpoint;
use crate::net::message::{Message, U32_SIZE};
use crate::net::NetworkErrorKind;

pub(crate) struct AsyncTcpConnection {
    tcp_stream: TcpStream,
}

impl AsyncTcpConnection {
    pub(crate) async fn establish_with(endpoint: &Endpoint) -> Result<AsyncTcpConnection, Error> {
        debug!("establishing connection with {}", endpoint.address());
        TcpStream::connect(endpoint.address())
            .await
            .map(|tcp_stream| AsyncTcpConnection { tcp_stream })
    }

    pub(crate) fn new(tcp_stream: TcpStream) -> AsyncTcpConnection {
        AsyncTcpConnection { tcp_stream }
    }

    pub(crate) async fn read(&mut self) -> Result<Message, NetworkErrorKind> {
        let mut message_size: [u8; U32_SIZE] = [0; U32_SIZE];
        let _ = self.tcp_stream.peek(&mut message_size).await?;

        let message_size = u32::from_be_bytes(message_size) as usize;
        let mut message = Vec::with_capacity(message_size + U32_SIZE);

        let _ = self.tcp_stream.read_buf(&mut message).await?;
        Ok(Message::deserialize_from(&message[..])?)
    }

    pub(crate) async fn write(&mut self, message: &Message) -> Result<(), NetworkErrorKind> {
        let serialized = message.serialize()?;
        self.tcp_stream.write_all(&serialized).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use tokio::net::TcpListener;

    use crate::net::connection::AsyncTcpConnection;
    use crate::net::endpoint::Endpoint;
    use crate::net::message::Message;

    #[tokio::test]
    async fn read_from_connection_successfully() {
        let listener_result = TcpListener::bind("localhost:9009").await;
        assert!(listener_result.is_ok());

        let handle = tokio::spawn(async move {
            let tcp_listener = listener_result.unwrap();
            let stream = tcp_listener.accept().await.unwrap();

            let mut connection = AsyncTcpConnection::new(stream.0);
            let message = connection.read().await.unwrap();

            assert!(message.is_find_value_type());
            if let Message::FindValue { key, .. } = message {
                assert_eq!("Kademlia".to_string(), String::from_utf8(key).unwrap());
            }
        });

        let tcp_connection_result =
            AsyncTcpConnection::establish_with(&Endpoint::new("localhost".to_string(), 9009)).await;
        assert!(tcp_connection_result.is_ok());

        let mut tcp_connection = tcp_connection_result.unwrap();
        let payload = Message::find_value_type(b"Kademlia".to_vec());

        let write_result = tcp_connection.write(&payload).await;
        assert!(write_result.is_ok());

        handle.await.unwrap();
    }

    #[tokio::test]
    async fn write_to_connection_successfully() {
        let listener_result = TcpListener::bind("localhost:9898").await;
        assert!(listener_result.is_ok());

        let tcp_connection_result =
            AsyncTcpConnection::establish_with(&Endpoint::new("localhost".to_string(), 9898)).await;
        assert!(tcp_connection_result.is_ok());

        let mut tcp_connection = tcp_connection_result.unwrap();
        let payload = Message::find_value_type(b"Kademlia".to_vec());

        let write_result = tcp_connection.write(&payload).await;
        assert!(write_result.is_ok());
    }

    #[tokio::test]
    async fn connect_to_endpoint_fails() {
        let tcp_connection_result =
            AsyncTcpConnection::establish_with(&Endpoint::new("localhost".to_string(), 1010)).await;
        assert!(tcp_connection_result.is_err());
    }
}
