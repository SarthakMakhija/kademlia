use std::collections::HashMap;
use std::sync::Arc;

use log::{error, info, warn};
use tokio::sync::mpsc::error::SendError;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{mpsc, oneshot};

use crate::executor::message_action::{FindNodeMessageAction, FindValueMessageAction, MessageAction, SendPingReplyMessageAction, StoreKeyValueMessageAction};
use crate::executor::response::{ChanneledMessage, MessageResponse, MessageStatus};
use crate::net::message::{Message, MessageTypes};
use crate::net::node::Node;
use crate::net::wait::WaitingList;
use crate::net::AsyncNetwork;
use crate::routing::Table;
use crate::store::Store;

pub(crate) struct MessageExecutor {
    sender: Sender<ChanneledMessage>,
    waiting_list: Arc<WaitingList>,
    async_network: Arc<AsyncNetwork>,
}

impl MessageExecutor {
    pub(crate) fn new(
        current_node: Node,
        store: Arc<dyn Store>,
        waiting_list: Arc<WaitingList>,
        routing_table: Arc<Table>,
    ) -> Self {
        //TODO: make 100 configurable
        let (sender, receiver) = mpsc::channel(100);

        let executor = MessageExecutor {
            sender,
            waiting_list: waiting_list.clone(),
            async_network: AsyncNetwork::new(waiting_list),
        };
        executor.start(current_node, receiver, store, routing_table);
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
        routing_table: Arc<Table>,
    ) {
        let mut action_by_message: HashMap<MessageTypes, Box<dyn MessageAction>> = HashMap::new();
        action_by_message.insert(
            MessageTypes::Store,
            StoreKeyValueMessageAction::new(store.clone()),
        );
        action_by_message.insert(
            MessageTypes::Ping,
            SendPingReplyMessageAction::new(current_node, self.async_network.clone()),
        );
        action_by_message.insert(
            MessageTypes::FindValue,
            FindValueMessageAction::new(store, routing_table.clone(), self.async_network.clone())
        );
        action_by_message.insert(
            MessageTypes::FindNode,
            FindNodeMessageAction::new(routing_table, self.async_network.clone())
        );

        let waiting_list = self.waiting_list.clone();
        tokio::spawn(async move {
            loop {
                match receiver.recv().await {
                    Some(channeled_message) => match channeled_message.message {
                        Message::Store { .. } => {
                            info!("working on store message in MessageExecutor");
                            action_by_message
                                .get(&MessageTypes::Store)
                                .unwrap()
                                .act_on(channeled_message.message.clone())
                                .await;

                            let _ = channeled_message.send_response(MessageStatus::StoreDone);
                        }
                        Message::FindValue { .. } => {
                            info!("working on findValue message in MessageExecutor");
                            action_by_message
                                .get(&MessageTypes::FindValue)
                                .unwrap()
                                .act_on(channeled_message.message.clone())
                                .await;

                            let _ = channeled_message.send_response(MessageStatus::FindValueDone);
                        }
                        Message::FindNode { .. } => {
                            info!("working on findNode message in MessageExecutor");
                            action_by_message
                                .get(&MessageTypes::FindNode)
                                .unwrap()
                                .act_on(channeled_message.message.clone())
                                .await;

                            let _ = channeled_message.send_response(MessageStatus::FindValueDone);
                        }
                        Message::Ping { .. } => {
                            info!("working on ping message in MessageExecutor");
                            action_by_message
                                .get(&MessageTypes::Ping)
                                .unwrap()
                                .act_on(channeled_message.message.clone())
                                .await;

                            let _ = channeled_message.send_response(MessageStatus::PingDone);
                        }
                        Message::PingReply { message_id, ..} |
                        Message::FindValueReply { message_id, ..} |
                        Message::FindNodeReply { message_id, .. }=> {
                            info!("working on a reply message in MessageExecutor");
                            waiting_list.handle_response(message_id, Ok(channeled_message.message.clone()));

                            let _ = channeled_message.send_response(MessageStatus::ReplyDone);
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
            }
        });
    }
}

#[cfg(test)]
mod store_message_executor {
    use std::sync::Arc;
    use std::time::Duration;

    use crate::executor::message::MessageExecutor;
    use crate::id::Id;
    use crate::net::endpoint::Endpoint;
    use crate::net::message::Message;
    use crate::net::node::Node;
    use crate::net::wait::{WaitingList, WaitingListOptions};
    use crate::routing::Table;
    use crate::store::{InMemoryStore, Store};
    use crate::time::SystemClock;

    #[tokio::test]
    async fn submit_store_message_successfully() {
        let store = Arc::new(InMemoryStore::new());
        let node = Node::new_with_id(
            Endpoint::new("localhost".to_string(), 9090),
            Id::new(255u16.to_be_bytes().to_vec()),
        );
        let node_id = node.node_id();

        let executor = MessageExecutor::new(node, store.clone(), waiting_list(), Arc::new(Table::new(node_id)));
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
        let node_id = node.node_id();
        let executor = MessageExecutor::new(node, store.clone(), waiting_list(), Arc::new(Table::new(node_id)));

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
        let node_id = node.node_id();
        let executor = MessageExecutor::new(node, store.clone(), waiting_list(), Arc::new(Table::new(node_id)));

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
    async fn submit_two_store_messages() {
        let store = Arc::new(InMemoryStore::new());
        let node = Node::new_with_id(
            Endpoint::new("localhost".to_string(), 9090),
            Id::new(255u16.to_be_bytes().to_vec()),
        );

        let node_id = node.node_id();
        let executor = Arc::new(MessageExecutor::new(node, store.clone(), waiting_list(), Arc::new(Table::new(node_id))));
        let executor_clone = executor.clone();
        let store_clone = store.clone();

        let handle = tokio::spawn(async move {
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

            let value = store.get(&"kademlia".as_bytes().to_vec());
            assert!(value.is_some());

            assert_eq!(
                "distributed hash table",
                String::from_utf8(value.unwrap()).unwrap()
            );
        });

        let other_handle = tokio::spawn(async move {
            let submit_result = executor_clone
                .submit(Message::store_type(
                    "store type".as_bytes().to_vec(),
                    "in-memory".as_bytes().to_vec(),
                    Node::new(Endpoint::new("localhost".to_string(), 1909)),
                ))
                .await;
            assert!(submit_result.is_ok());

            let message_response = submit_result.unwrap();
            let message_response_result = message_response.wait_until_response_is_received().await;
            assert!(message_response_result.is_ok());

            let value = store_clone.get(&"store type".as_bytes().to_vec());
            assert!(value.is_some());

            assert_eq!("in-memory", String::from_utf8(value.unwrap()).unwrap());
        });

        handle.await.unwrap();
        other_handle.await.unwrap();
    }

    #[tokio::test]
    async fn submit_a_message_after_shutdown() {
        let store = Arc::new(InMemoryStore::new());
        let node = Node::new_with_id(
            Endpoint::new("localhost".to_string(), 9090),
            Id::new(255u16.to_be_bytes().to_vec()),
        );
        let node_id = node.node_id();
        let executor = MessageExecutor::new(node, store.clone(), waiting_list(), Arc::new(Table::new(node_id)));

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

    fn waiting_list() -> Arc<WaitingList> {
        WaitingList::new(
            WaitingListOptions::new(Duration::from_secs(120), Duration::from_millis(100)),
            SystemClock::new(),
        )
    }
}

#[cfg(test)]
mod ping_message_executor {
    use std::sync::Arc;
    use std::time::Duration;

    use tokio::net::TcpListener;

    use crate::executor::message::MessageExecutor;
    use crate::executor::message::ping_message_executor::setup::TestCallback;
    use crate::net::connection::AsyncTcpConnection;
    use crate::net::endpoint::Endpoint;
    use crate::net::message::{Message, MessageId};
    use crate::net::node::Node;
    use crate::net::wait::{WaitingList, WaitingListOptions};
    use crate::routing::Table;
    use crate::store::InMemoryStore;
    use crate::time::SystemClock;

    mod setup {
        use crate::net::callback::{Callback, ResponseError};
        use std::any::Any;
        use std::sync::{Arc, Mutex};

        use crate::net::message::Message;

        pub(crate) struct TestCallback {
           pub(crate) responses: Mutex<Vec<Message>>,
        }

        impl TestCallback {
            pub(crate) fn new() -> Arc<TestCallback> {
                Arc::new(TestCallback {
                    responses: Mutex::new(Vec::new()),
                })
            }
        }

        impl Callback for TestCallback {
            fn on_response(&self, response: Result<Message, ResponseError>) {
                if response.is_ok() {
                    self.responses.lock().unwrap().push(response.unwrap());
                }
            }

            fn as_any(&self) -> &dyn Any {
                self
            }
        }
    }

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
            if let Message::PingReply { current_node: from, .. } = message {
                assert_eq!("localhost:9090", from.endpoint().address());
            }
        });

        let store = Arc::new(InMemoryStore::new());
        let node = Node::new(Endpoint::new("localhost".to_string(), 9090));
        let node_id = node.node_id();
        let executor = MessageExecutor::new(node, store.clone(), waiting_list(), Arc::new(Table::new(node_id)));

        let node_sending_ping = Node::new(Endpoint::new("localhost".to_string(), 7565));
        let mut ping_message = Message::ping_type(node_sending_ping);
        ping_message.set_message_id(10);

        let submit_result = executor.submit(ping_message).await;

        assert!(submit_result.is_ok());

        let message_response = submit_result.unwrap();
        let message_response_result = message_response.wait_until_response_is_received().await;
        assert!(message_response_result.is_ok());

        handle.await.unwrap();
    }

    #[tokio::test]
    async fn submit_ping_reply_message() {
        let store = Arc::new(InMemoryStore::new());
        let node = Node::new(Endpoint::new("localhost".to_string(), 9090));

        let node_id = node.node_id();
        let waiting_list = waiting_list();
        let executor = MessageExecutor::new(node.clone(), store.clone(), waiting_list.clone(), Arc::new(Table::new(node_id)));

        let message_id: MessageId = 100;
        let callback = TestCallback::new();
        waiting_list.add(message_id, callback.clone());

        let ping_reply_message = Message::ping_reply_type(node, message_id);

        let submit_result = executor.submit(ping_reply_message).await;
        assert!(submit_result.is_ok());

        let _ = submit_result.unwrap().wait_until_response_is_received().await.unwrap();

        let response_guard = callback.responses.lock().unwrap();
        let message = response_guard.get(0).unwrap();

        assert!(message.is_ping_reply_type());
    }

    fn waiting_list() -> Arc<WaitingList> {
        WaitingList::new(
            WaitingListOptions::new(Duration::from_secs(120), Duration::from_millis(100)),
            SystemClock::new(),
        )
    }
}

#[cfg(test)]
mod find_value_message_executor {
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::net::TcpListener;
    use crate::executor::message::find_value_message_executor::setup::TestCallback;
    use crate::executor::message::MessageExecutor;
    use crate::id::Id;
    use crate::net::connection::AsyncTcpConnection;
    use crate::net::endpoint::Endpoint;
    use crate::net::message::{Message, MessageId};
    use crate::net::node::Node;
    use crate::net::wait::{WaitingList, WaitingListOptions};
    use crate::routing::Table;
    use crate::store::{InMemoryStore, Key, Store};
    use crate::time::SystemClock;

    mod setup {
        use crate::net::callback::{Callback, ResponseError};
        use std::any::Any;
        use std::sync::{Arc, Mutex};

        use crate::net::message::Message;

        pub(crate) struct TestCallback {
            pub(crate) responses: Mutex<Vec<Message>>,
        }

        impl TestCallback {
            pub(crate) fn new() -> Arc<TestCallback> {
                Arc::new(TestCallback {
                    responses: Mutex::new(Vec::new()),
                })
            }
        }

        impl Callback for TestCallback {
            fn on_response(&self, response: Result<Message, ResponseError>) {
                if response.is_ok() {
                    self.responses.lock().unwrap().push(response.unwrap());
                }
            }

            fn as_any(&self) -> &dyn Any {
                self
            }
        }
    }

    #[tokio::test]
    async fn submit_find_value_message_with_the_value_in_store() {
        let listener_result = TcpListener::bind("localhost:9818").await;
        assert!(listener_result.is_ok());

        let handle = tokio::spawn(async move {
            let tcp_listener = listener_result.unwrap();
            let stream = tcp_listener.accept().await.unwrap();

            let mut connection = AsyncTcpConnection::new(stream.0);
            let message = connection.read().await.unwrap();

            assert!(message.is_find_value_reply_type());
            if let Message::FindValueReply { value, .. } = message {
                assert_eq!(value.unwrap(), "distributed hash table".as_bytes().to_vec());
            }
        });

        let store = Arc::new(InMemoryStore::new());
        store.put_or_update(Key::new("kademlia".as_bytes().to_vec()), "distributed hash table".as_bytes().to_vec());

        let node = Node::new_with_id(
            Endpoint::new("localhost".to_string(), 9090),
            Id::new(255u16.to_be_bytes().to_vec()),
        );
        let node_id = node.node_id();

        let executor = MessageExecutor::new(node, store.clone(), waiting_list(), Arc::new(Table::new(node_id)));

        let mut find_value_message = Message::find_value_type(
            Node::new(Endpoint::new("localhost".to_string(), 9818)), "kademlia".as_bytes().to_vec());
        find_value_message.set_message_id(100);

        let submit_result = executor.submit(find_value_message).await;
        assert!(submit_result.is_ok());

        submit_result.unwrap().wait_until_response_is_received().await.unwrap();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn submit_find_value_reply() {
        let store = Arc::new(InMemoryStore::new());
        let node = Node::new(Endpoint::new("localhost".to_string(), 9090));

        let node_id = node.node_id();
        let waiting_list = waiting_list();
        let executor = MessageExecutor::new(node.clone(), store.clone(), waiting_list.clone(), Arc::new(Table::new(node_id)));

        let message_id: MessageId = 100;
        let callback = TestCallback::new();
        waiting_list.add(message_id, callback.clone());

        let find_value_reply = Message::find_value_reply_type(
            message_id, Some("kademlia".as_bytes().to_vec()), None
        );

        let submit_result = executor.submit(find_value_reply).await;
        assert!(submit_result.is_ok());

        let _ = submit_result.unwrap().wait_until_response_is_received().await.unwrap();

        let response_guard = callback.responses.lock().unwrap();
        let message = response_guard.get(0).unwrap();

        assert!(message.is_find_value_reply_type());
    }

    fn waiting_list() -> Arc<WaitingList> {
        WaitingList::new(
            WaitingListOptions::new(Duration::from_secs(120), Duration::from_millis(100)),
            SystemClock::new(),
        )
    }
}

#[cfg(test)]
mod find_node_message_executor {
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::net::TcpListener;
    use crate::executor::message::find_node_message_executor::setup::TestCallback;
    use crate::executor::message::MessageExecutor;
    use crate::id::Id;
    use crate::net::connection::AsyncTcpConnection;
    use crate::net::endpoint::Endpoint;
    use crate::net::message::{Message, MessageId};
    use crate::net::node::Node;
    use crate::net::wait::{WaitingList, WaitingListOptions};
    use crate::routing::Table;
    use crate::store::InMemoryStore;
    use crate::time::SystemClock;

    mod setup {
        use crate::net::callback::{Callback, ResponseError};
        use std::any::Any;
        use std::sync::{Arc, Mutex};

        use crate::net::message::Message;

        pub(crate) struct TestCallback {
            pub(crate) responses: Mutex<Vec<Message>>,
        }

        impl TestCallback {
            pub(crate) fn new() -> Arc<TestCallback> {
                Arc::new(TestCallback {
                    responses: Mutex::new(Vec::new()),
                })
            }
        }

        impl Callback for TestCallback {
            fn on_response(&self, response: Result<Message, ResponseError>) {
                if response.is_ok() {
                    self.responses.lock().unwrap().push(response.unwrap());
                }
            }

            fn as_any(&self) -> &dyn Any {
                self
            }
        }
    }

    #[tokio::test]
    async fn submit_find_node_message() {
        let listener_result = TcpListener::bind("localhost:9808").await;
        assert!(listener_result.is_ok());

        let handle = tokio::spawn(async move {
            let tcp_listener = listener_result.unwrap();
            let stream = tcp_listener.accept().await.unwrap();

            let mut connection = AsyncTcpConnection::new(stream.0);
            let message = connection.read().await.unwrap();

            assert!(message.is_find_node_reply_type());
            if let Message::FindNodeReply { neighbors, .. } = message {
                assert_eq!(1, neighbors.len());
                assert_eq!("localhost:7070", neighbors[0].endpoint().address());
            }
        });

        let node = Node::new_with_id(
            Endpoint::new("localhost".to_string(), 9808),
            Id::new(255u16.to_be_bytes().to_vec()),
        );

        let store = Arc::new(InMemoryStore::new());
        let node_id = node.node_id();
        let routing_table = Arc::new(Table::new(node_id));
        let executor = MessageExecutor::new(node, store, waiting_list(), routing_table.clone());
        routing_table.add(
            Node::new_with_id(
                Endpoint::new("localhost".to_string(), 7070),
                Id::new(247u16.to_be_bytes().to_vec()),
            )
        );

        let mut find_node_message = Message::find_node_type(
            Node::new(Endpoint::new("localhost".to_string(), 9808)),
            Id::new(255u16.to_be_bytes().to_vec())
        );
        find_node_message.set_message_id(100);

        let submit_result = executor.submit(find_node_message).await;
        assert!(submit_result.is_ok());

        submit_result.unwrap().wait_until_response_is_received().await.unwrap();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn submit_find_node_reply() {
        let store = Arc::new(InMemoryStore::new());
        let node = Node::new(Endpoint::new("localhost".to_string(), 9090));

        let node_id = node.node_id();
        let waiting_list = waiting_list();
        let executor = MessageExecutor::new(node.clone(), store, waiting_list.clone(), Arc::new(Table::new(node_id)));

        let message_id: MessageId = 100;
        let callback = TestCallback::new();
        waiting_list.add(message_id, callback.clone());

        let closest_neighbors = Vec::new();
        let find_value_reply = Message::find_node_reply_type(
            message_id, closest_neighbors
        );

        let submit_result = executor.submit(find_value_reply).await;
        assert!(submit_result.is_ok());

        let _ = submit_result.unwrap().wait_until_response_is_received().await.unwrap();

        let response_guard = callback.responses.lock().unwrap();
        let message= response_guard.get(0).unwrap();

        assert!(message.is_find_node_reply_type());
    }

    fn waiting_list() -> Arc<WaitingList> {
        WaitingList::new(
            WaitingListOptions::new(Duration::from_secs(120), Duration::from_millis(100)),
            SystemClock::new(),
        )
    }
}
