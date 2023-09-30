use std::fmt::{Display, Formatter};
use std::io::Error;

use crate::message::Message;
use crate::net::connection::AsyncTcpConnection;
use crate::net::endpoint::Endpoint;

pub(crate) mod connection;
pub(crate) mod endpoint;
pub(crate) mod node;

#[derive(Debug)]
pub(crate) enum NetworkErrorKind {
    Io(Error),
    SerializationError(String),
}

impl From<Error> for NetworkErrorKind {
    fn from(err: Error) -> Self {
        NetworkErrorKind::Io(err)
    }
}

impl From<bincode::Error> for NetworkErrorKind {
    fn from(value: bincode::Error) -> Self {
        NetworkErrorKind::SerializationError(value.to_string())
    }
}

impl Display for NetworkErrorKind {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            NetworkErrorKind::Io(err) => write!(formatter, "io error: {}", err),
            NetworkErrorKind::SerializationError(description) => {
                write!(formatter, "serialization err: {}", description)
            }
        }
    }
}

pub(crate) struct AsyncNetwork;

impl AsyncNetwork {
    async fn send(message: Message, endpoint: &Endpoint) -> Result<(), NetworkErrorKind> {
        let mut tcp_connection = AsyncTcpConnection::establish_with(endpoint).await?;
        let serialized = message.serialize()?;
        tcp_connection.write(&serialized).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::message::Message;
    use crate::net::endpoint::Endpoint;
    use crate::net::AsyncNetwork;
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn send_message_successfully() {
        let listener_result = TcpListener::bind("localhost:8989").await;
        assert!(listener_result.is_ok());

        let network_send_result = AsyncNetwork::send(
            Message::store_type(
                "kademlia".as_bytes().to_vec(),
                "distributed hash table".as_bytes().to_vec(),
            ),
            &Endpoint::new("localhost".to_string(), 8989),
        )
        .await;
        assert!(network_send_result.is_ok());
    }
}
