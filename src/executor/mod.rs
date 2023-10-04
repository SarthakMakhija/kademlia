use std::sync::Arc;

use log::{error, info, warn};
use tokio::sync::mpsc::error::SendError;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{mpsc, oneshot};

use crate::executor::message_action::{MessageAction, PingMessageAction, StoreMessageAction};
use crate::executor::response::{ChanneledMessage, MessageResponse, MessageStatus};
use crate::net::message::Message;
use crate::net::node::Node;
use crate::net::AsyncNetwork;
use crate::routing::Table;
use crate::store::Store;

mod message_action;
mod response;

pub(crate) struct MessageExecutor {
    sender: Sender<ChanneledMessage>,
    routing_table: Arc<Table>,
    async_network: Arc<AsyncNetwork>,
}

impl MessageExecutor {
    pub(crate) fn new(current_node: Node, store: Arc<dyn Store>) -> Self {
        //TODO: make 100 configurable
        let (sender, receiver) = mpsc::channel(100);

        let executor = MessageExecutor {
            sender,
            routing_table: Arc::new(Table::new(current_node.node_id())),
            async_network: Arc::new(AsyncNetwork::new()),
        };
        executor.start(current_node, receiver, store);
        executor
    }

    pub(crate) async fn submit(
        &self,
        message: Message,
    ) -> Result<MessageResponse, SendError<ChanneledMessage>> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(ChanneledMessage::new(message, sender))
            .await
            .map(|_| MessageResponse::new(receiver))
    }

    pub(crate) async fn shutdown(&self) -> Result<MessageResponse, SendError<ChanneledMessage>> {
        self.submit(Message::shutdown_type()).await
    }

    fn start(
        &self,
        current_node: Node,
        mut receiver: Receiver<ChanneledMessage>,
        store: Arc<dyn Store>,
    ) {
        let routing_table = self.routing_table.clone();
        let async_network = self.async_network.clone();

        tokio::spawn(async move {
            match receiver.recv().await {
                Some(channeled_message) => match channeled_message.message {
                    Message::Store { .. } => {
                        info!("working on store message in MessageExecutor");
                        let action = StoreMessageAction::new(&store, &routing_table);
                        action.act_on(channeled_message.message.clone());

                        let _ = channeled_message.send_response(MessageStatus::StoreDone);
                    }
                    Message::Ping { .. } => {
                        info!("working on ping message in MessageExecutor");
                        let action = PingMessageAction::new(&current_node, &async_network);
                        action.act_on(channeled_message.message.clone());

                        let _ = channeled_message.send_response(MessageStatus::PingDone);
                    }
                    Message::ShutDown => {
                        drop(receiver);
                        warn!("shutting down MessageExecutor, received shutdown message");

                        let _ = channeled_message.send_response(MessageStatus::ShutdownDone);
                        return;
                    }
                    //TODO: Handle
                    _ => {}
                },
                None => {
                    error!("did not receive any more message in MessageExecutor. Looks like the sender was dropped");
                    return;
                }
            }
        });
    }
}

#[cfg(test)]
mod store_message_executor {
    use std::sync::Arc;

    use crate::executor::MessageExecutor;
    use crate::id::Id;
    use crate::net::endpoint::Endpoint;
    use crate::net::message::Message;
    use crate::net::node::Node;
    use crate::store::{InMemoryStore, Store};

    #[tokio::test]
    async fn submit_store_message_successfully() {
        let store = Arc::new(InMemoryStore::new());
        let node = Node::new_with_id(
            Endpoint::new("localhost".to_string(), 9090),
            Id::new(255u16.to_be_bytes().to_vec()),
        );
        let executor = MessageExecutor::new(node, store.clone());
        let submit_result = executor
            .submit(Message::store_type(
                "kademlia".as_bytes().to_vec(),
                "distributed hash table".as_bytes().to_vec(),
                Node::new(Endpoint::new("localhost".to_string(), 9090)),
            ))
            .await;
        assert!(submit_result.is_ok());
    }

    #[tokio::test]
    async fn submit_store_message_with_successful_message_store() {
        let store = Arc::new(InMemoryStore::new());

        let node = Node::new_with_id(
            Endpoint::new("localhost".to_string(), 9090),
            Id::new(255u16.to_be_bytes().to_vec()),
        );
        let executor = MessageExecutor::new(node, store.clone());

        let submit_result = executor
            .submit(Message::store_type(
                "kademlia".as_bytes().to_vec(),
                "distributed hash table".as_bytes().to_vec(),
                Node::new(Endpoint::new("localhost".to_string(), 1909)),
            ))
            .await;
        assert!(submit_result.is_ok());

        let message_response = submit_result.unwrap();
        let message_response_result = message_response.wait_until_response_is_received().await;
        assert!(message_response_result.is_ok());

        let message_status = message_response_result.unwrap();
        assert!(message_status.is_store_done());
    }

    #[tokio::test]
    async fn submit_store_message_with_successful_value_in_store() {
        let store = Arc::new(InMemoryStore::new());
        let node = Node::new_with_id(
            Endpoint::new("localhost".to_string(), 9090),
            Id::new(255u16.to_be_bytes().to_vec()),
        );
        let executor = MessageExecutor::new(node, store.clone());

        let submit_result = executor
            .submit(Message::store_type(
                "kademlia".as_bytes().to_vec(),
                "distributed hash table".as_bytes().to_vec(),
                Node::new(Endpoint::new("localhost".to_string(), 1909)),
            ))
            .await;
        assert!(submit_result.is_ok());

        let message_response = submit_result.unwrap();
        let message_response_result = message_response.wait_until_response_is_received().await;
        assert!(message_response_result.is_ok());

        let message_status = message_response_result.unwrap();
        assert!(message_status.is_store_done());

        let value = store.get(&"kademlia".as_bytes().to_vec());
        assert!(value.is_some());

        assert_eq!(
            "distributed hash table",
            String::from_utf8(value.unwrap()).unwrap()
        );
    }

    #[tokio::test]
    async fn submit_store_message_with_addition_of_node_in_routing_table() {
        let store = Arc::new(InMemoryStore::new());
        let node = Node::new_with_id(
            Endpoint::new("localhost".to_string(), 9090),
            Id::new(255u16.to_be_bytes().to_vec()),
        );
        let executor = MessageExecutor::new(node, store);

        let submit_result = executor
            .submit(Message::store_type(
                "kademlia".as_bytes().to_vec(),
                "distributed hash table".as_bytes().to_vec(),
                Node::new(Endpoint::new("localhost".to_string(), 1909)),
            ))
            .await;
        assert!(submit_result.is_ok());

        let message_response = submit_result.unwrap();
        let message_response_result = message_response.wait_until_response_is_received().await;
        assert!(message_response_result.is_ok());

        let message_status = message_response_result.unwrap();
        assert!(message_status.is_store_done());

        let node = &Node::new(Endpoint::new("localhost".to_string(), 1909));
        let (_, contains) = executor.routing_table.contains(node);
        assert!(contains);
    }

    #[tokio::test]
    async fn submit_a_message_after_shutdown() {
        let store = Arc::new(InMemoryStore::new());
        let node = Node::new_with_id(
            Endpoint::new("localhost".to_string(), 9090),
            Id::new(255u16.to_be_bytes().to_vec()),
        );
        let executor = MessageExecutor::new(node, store.clone());

        let submit_result = executor.shutdown().await;
        assert!(submit_result.is_ok());

        let message_response = submit_result.unwrap();
        let message_response_result = message_response.wait_until_response_is_received().await;
        assert!(message_response_result.is_ok());

        let submit_result = executor
            .submit(Message::store_type(
                "kademlia".as_bytes().to_vec(),
                "distributed hash table".as_bytes().to_vec(),
                Node::new(Endpoint::new("localhost".to_string(), 1909)),
            ))
            .await;
        assert!(submit_result.is_err());
    }
}

#[cfg(test)]
mod ping_message_executor {
    use std::sync::Arc;

    use tokio::net::TcpListener;

    use crate::executor::MessageExecutor;
    use crate::net::connection::AsyncTcpConnection;
    use crate::net::endpoint::Endpoint;
    use crate::net::message::Message;
    use crate::net::node::Node;
    use crate::store::InMemoryStore;

    #[tokio::test]
    async fn submit_ping_message_with_successful_reply() {
        let listener_result = TcpListener::bind("localhost:7565").await;
        assert!(listener_result.is_ok());

        let handle = tokio::spawn(async move {
            let tcp_listener = listener_result.unwrap();
            let stream = tcp_listener.accept().await.unwrap();

            let mut connection = AsyncTcpConnection::new(stream.0);
            let message = connection.read().await.unwrap();

            assert!(message.is_ping_reply_type());
            if let Message::SendPingReply { to, .. } = message {
                assert_eq!("localhost:9090", to.endpoint().address());
            }
        });

        let store = Arc::new(InMemoryStore::new());
        let node = Node::new(Endpoint::new("localhost".to_string(), 9090));
        let executor = MessageExecutor::new(node, store.clone());

        let node_sending_ping = Node::new(Endpoint::new("localhost".to_string(), 7565));
        let submit_result = executor.submit(Message::ping_type(node_sending_ping)).await;
        assert!(submit_result.is_ok());

        let message_response = submit_result.unwrap();
        let message_response_result = message_response.wait_until_response_is_received().await;
        assert!(message_response_result.is_ok());

        handle.await.unwrap();
    }
}
