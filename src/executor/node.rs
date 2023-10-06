use std::collections::HashMap;
use std::sync::Arc;

use log::{error, info, warn};
use tokio::sync::mpsc::error::SendError;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{mpsc, oneshot};

use crate::executor::message_action::{AddNodeAction, MessageAction};
use crate::executor::response::{ChanneledMessage, MessageResponse, MessageStatus};
use crate::net::message::{Message, MessageTypes};
use crate::net::node::Node;
use crate::routing::Table;

pub(crate) struct AddNodeExecutor {
    sender: Sender<ChanneledMessage>,
    routing_table: Arc<Table>,
}

impl AddNodeExecutor {
    pub(crate) fn new(current_node: Node) -> Self {
        //TODO: make 100 configurable
        let (sender, receiver) = mpsc::channel(100);

        let executor = AddNodeExecutor {
            sender,
            routing_table: Arc::new(Table::new(current_node.node_id())),
        };
        executor.start(receiver);
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

    fn start(&self, mut receiver: Receiver<ChanneledMessage>) {
        let routing_table = self.routing_table.clone();

        let mut action_by_message: HashMap<MessageTypes, Box<dyn MessageAction>> = HashMap::new();
        action_by_message.insert(
            MessageTypes::AddNode,
            Box::new(AddNodeAction::new(routing_table)),
        );

        tokio::spawn(async move {
            loop {
                match receiver.recv().await {
                    Some(channeled_message) => match channeled_message.message {
                        Message::AddNode { .. } => {
                            info!("working on add node message in AddNodeExecutor");
                            action_by_message
                                .get(&MessageTypes::AddNode)
                                .unwrap()
                                .act_on(channeled_message.message.clone())
                                .await;

                            let _ = channeled_message.send_response(MessageStatus::AddNodeDone);
                        }
                        Message::ShutDown => {
                            drop(receiver);
                            warn!("shutting down AddNodeExecutor, received shutdown message");

                            let _ = channeled_message.send_response(MessageStatus::ShutdownDone);
                            return;
                        }
                        //TODO: Handle
                        _ => {}
                    },
                    None => {
                        error!("did not receive any more message in AddNodeExecutor. Looks like the sender was dropped");
                        return;
                    }
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use crate::executor::node::AddNodeExecutor;
    use crate::id::Id;
    use crate::net::endpoint::Endpoint;
    use crate::net::message::Message;
    use crate::net::node::Node;
    use std::sync::Arc;

    #[tokio::test]
    async fn submit_add_node_message_successfully() {
        let node = Node::new_with_id(
            Endpoint::new("localhost".to_string(), 9090),
            Id::new(255u16.to_be_bytes().to_vec()),
        );
        let executor = AddNodeExecutor::new(node);
        let submit_result = executor
            .submit(Message::add_node_type(Node::new(Endpoint::new(
                "localhost".to_string(),
                9090,
            ))))
            .await;

        assert!(submit_result.is_ok());
    }

    #[tokio::test]
    async fn submit_add_node_message_successfully_with_node_addition_in_routing_table() {
        let node = Node::new_with_id(
            Endpoint::new("localhost".to_string(), 9090),
            Id::new(255u16.to_be_bytes().to_vec()),
        );
        let executor = AddNodeExecutor::new(node);
        let submit_result = executor
            .submit(Message::add_node_type(Node::new(Endpoint::new(
                "localhost".to_string(),
                8989,
            ))))
            .await;

        assert!(submit_result.is_ok());

        let message_response_result = submit_result
            .unwrap()
            .wait_until_response_is_received()
            .await;
        assert!(message_response_result.is_ok());

        let node = Node::new(Endpoint::new("localhost".to_string(), 8989));
        let (_, contains) = executor.routing_table.contains(&node);

        assert!(contains);
    }

    #[tokio::test]
    async fn submit_two_add_node_messages_successfully() {
        let node = Node::new_with_id(
            Endpoint::new("localhost".to_string(), 9090),
            Id::new(255u16.to_be_bytes().to_vec()),
        );
        let executor = Arc::new(AddNodeExecutor::new(node));
        let executor_clone = executor.clone();

        let handle = tokio::spawn(async move {
            let submit_result = executor
                .submit(Message::add_node_type(Node::new(Endpoint::new(
                    "localhost".to_string(),
                    8989,
                ))))
                .await;

            assert!(submit_result.is_ok());

            let message_response_result = submit_result
                .unwrap()
                .wait_until_response_is_received()
                .await;
            assert!(message_response_result.is_ok());

            let node = Node::new(Endpoint::new("localhost".to_string(), 8989));
            let (_, contains) = executor.routing_table.contains(&node);

            assert!(contains);
        });

        let other_handle = tokio::spawn(async move {
            let submit_result = executor_clone
                .submit(Message::add_node_type(Node::new(Endpoint::new(
                    "localhost".to_string(),
                    7878,
                ))))
                .await;

            assert!(submit_result.is_ok());

            let message_response_result = submit_result
                .unwrap()
                .wait_until_response_is_received()
                .await;
            assert!(message_response_result.is_ok());

            let node = Node::new(Endpoint::new("localhost".to_string(), 7878));
            let (_, contains) = executor_clone.routing_table.contains(&node);

            assert!(contains);
        });

        handle.await.unwrap();
        other_handle.await.unwrap();
    }

    #[tokio::test]
    async fn submit_a_message_after_shutdown() {
        let node = Node::new_with_id(
            Endpoint::new("localhost".to_string(), 9090),
            Id::new(255u16.to_be_bytes().to_vec()),
        );
        let executor = AddNodeExecutor::new(node);

        let submit_result = executor.shutdown().await;
        assert!(submit_result.is_ok());

        let message_response = submit_result.unwrap();
        let message_response_result = message_response.wait_until_response_is_received().await;
        assert!(message_response_result.is_ok());

        let submit_result = executor
            .submit(Message::add_node_type(Node::new(Endpoint::new(
                "localhost".to_string(),
                1909,
            ))))
            .await;
        assert!(submit_result.is_err());
    }
}
