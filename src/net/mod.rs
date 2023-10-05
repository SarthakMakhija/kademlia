use crate::net::callback::ResponseAwaitingCallback;
use std::fmt::{Display, Formatter};
use std::io::Error;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

use crate::net::connection::AsyncTcpConnection;
use crate::net::endpoint::Endpoint;
use crate::net::message::{Message, MessageId};
use crate::net::wait::WaitingList;

pub(crate) mod callback;
pub(crate) mod connection;
pub(crate) mod endpoint;
pub(crate) mod message;
pub(crate) mod node;
pub(crate) mod wait;

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

pub(crate) struct AsyncNetwork {
    waiting_list: Arc<WaitingList>,
    next_message_id: AtomicI64,
}

impl AsyncNetwork {
    pub(crate) fn new(waiting_list: Arc<WaitingList>) -> Arc<Self> {
        Arc::new(AsyncNetwork {
            waiting_list,
            next_message_id: AtomicI64::new(1),
        })
    }

    pub(crate) async fn send(
        &self,
        message: Message,
        endpoint: &Endpoint,
    ) -> Result<(), NetworkErrorKind> {
        self.connect_and_write(message, endpoint).await
    }

    pub(crate) async fn send_with_message_id(
        &self,
        mut message: Message,
        endpoint: &Endpoint,
    ) -> Result<(), NetworkErrorKind> {
        message.set_message_id(self.generate_next_message_id());
        self.connect_and_write(message, endpoint).await
    }

    pub(crate) async fn send_with_message_id_expect_reply(
        &self,
        mut message: Message,
        endpoint: &Endpoint,
    ) -> Result<(), NetworkErrorKind> {
        let message_id = self.generate_next_message_id();
        message.set_message_id(message_id);

        let send_result = self.connect_and_write(message, endpoint).await;
        self.waiting_list
            .add(message_id, ResponseAwaitingCallback::new());

        send_result
    }

    async fn connect_and_write(
        &self,
        message: Message,
        endpoint: &Endpoint,
    ) -> Result<(), NetworkErrorKind> {
        let mut tcp_connection = AsyncTcpConnection::establish_with(endpoint).await?;
        tcp_connection.write(&message).await?;
        Ok(())
    }

    fn generate_next_message_id(&self) -> MessageId {
        self.next_message_id.fetch_add(1, Ordering::AcqRel) //TODO: confirm ordering
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use tokio::net::TcpListener;
    use tokio::task::JoinHandle;

    use crate::id::Id;
    use crate::net::connection::AsyncTcpConnection;
    use crate::net::endpoint::Endpoint;
    use crate::net::message::{Message, MessageId};
    use crate::net::node::Node;
    use crate::net::wait::{WaitingList, WaitingListOptions};
    use crate::net::AsyncNetwork;
    use crate::time::SystemClock;

    #[tokio::test]
    async fn send_message_successfully() {
        let listener_result = TcpListener::bind("localhost:8989").await;
        assert!(listener_result.is_ok());

        let network_send_result = AsyncNetwork::new(waiting_list())
            .send(
                Message::store_type(
                    "kademlia".as_bytes().to_vec(),
                    "distributed hash table".as_bytes().to_vec(),
                    Node::new_with_id(
                        Endpoint::new("localhost".to_string(), 2389),
                        Id::new(vec![10, 20]),
                    ),
                ),
                &Endpoint::new("localhost".to_string(), 8989),
            )
            .await;

        assert!(network_send_result.is_ok());
    }

    #[tokio::test]
    async fn send_message_with_id_successfully() {
        let listener_result = TcpListener::bind("localhost:2334").await;
        assert!(listener_result.is_ok());

        let handle = tokio::spawn(async move {
            let tcp_listener = listener_result.unwrap();
            let stream = tcp_listener.accept().await.unwrap();

            let mut connection = AsyncTcpConnection::new(stream.0);
            let message = connection.read().await.unwrap();

            assert!(message.is_ping_type());
            if let Message::Ping { message_id, .. } = message {
                assert_eq!(Some(1), message_id);
            }
        });

        let network_send_result = AsyncNetwork::new(waiting_list())
            .send_with_message_id(
                Message::ping_type(Node::new(Endpoint::new("localhost".to_string(), 5665))),
                &Endpoint::new("localhost".to_string(), 2334),
            )
            .await;

        assert!(network_send_result.is_ok());
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn send_message_with_id_expect_reply() {
        let listener_result = TcpListener::bind("localhost:2334").await;
        assert!(listener_result.is_ok());

        let waiting_list = waiting_list();
        let network_send_result = AsyncNetwork::new(waiting_list.clone())
            .send_with_message_id_expect_reply(
                Message::ping_type(Node::new(Endpoint::new("localhost".to_string(), 5665))),
                &Endpoint::new("localhost".to_string(), 2334),
            )
            .await;

        assert!(network_send_result.is_ok());
        assert!(waiting_list.contains(&1));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn generate_message_id() {
        let async_network = AsyncNetwork::new(waiting_list());
        let handles: Vec<JoinHandle<MessageId>> = (1..100)
            .map(|_| async_network.clone())
            .map(|async_network| {
                tokio::spawn(async move { async_network.generate_next_message_id() })
            })
            .collect();

        let mut message_ids = Vec::new();
        for handle in handles {
            message_ids.push(handle.await.unwrap());
        }
        message_ids.sort();

        assert_eq!((1..100).collect::<Vec<MessageId>>(), message_ids);
    }

    fn waiting_list() -> Arc<WaitingList> {
        Arc::new(WaitingList::new(
            WaitingListOptions::new(Duration::from_secs(120), Duration::from_millis(100)),
            SystemClock::new(),
        ))
    }
}
