use std::sync::Arc;

use async_trait::async_trait;
use log::warn;

use crate::net::{AsyncNetwork, NetworkErrorKind};
use crate::net::callback::{ResponseAwaitingCallback, ResponseStatus};
use crate::net::message::{Message, Source};
use crate::net::message::Message::AddNode;
use crate::net::node::Node;
use crate::routing::Table;
use crate::store::{Key, Store};

#[async_trait]
pub(crate) trait MessageAction: Send + Sync {
    async fn act_on(&self, message: Message);
}

pub(crate) struct StoreMessageAction {
    store: Arc<dyn Store>,
}

impl StoreMessageAction {
    pub(crate) fn new(store: Arc<dyn Store>) -> Self {
        StoreMessageAction { store }
    }
}

#[async_trait]
impl MessageAction for StoreMessageAction {
    async fn act_on(&self, message: Message) {
        if let Message::Store {
            key, key_id, value, ..
        } = message
        {
            self.store
                .put_or_update(Key::new_with_id(key, key_id), value);
        }
    }
}

pub(crate) struct PingMessageAction {
    current_node: Node,
    async_network: Arc<AsyncNetwork>,
}

impl PingMessageAction {
    pub(crate) fn new(current_node: Node, async_network: Arc<AsyncNetwork>) -> Self {
        PingMessageAction {
            current_node,
            async_network,
        }
    }
}

#[async_trait]
impl MessageAction for PingMessageAction {
    async fn act_on(&self, message: Message) {
        if let Message::Ping { message_id, from } = message {
            let current_node = self.current_node.clone();
            let async_network = self.async_network.clone();

            tokio::spawn(async move {
                assert!(message_id.is_some());
                let _ = async_network
                    .send(
                        Message::ping_reply_type(current_node, message_id.unwrap()),
                        from.endpoint(),
                    )
                    .await;
            });
        }
    }
}

pub(crate) struct FindValueMessageAction {
    store: Arc<dyn Store>,
    routing_table: Arc<Table>,
    async_network: Arc<AsyncNetwork>,
}

impl FindValueMessageAction {
    pub(crate) fn new(store: Arc<dyn Store>, routing_table: Arc<Table>,  async_network: Arc<AsyncNetwork>) -> Self {
        FindValueMessageAction {
            store,
            routing_table,
            async_network
        }
    }
}

#[async_trait]
impl MessageAction for FindValueMessageAction {
    async fn act_on(&self, message: Message) {
        if let Message::FindValue {source, message_id, key, key_id} = message {
            if message_id.is_none() {
                warn!("received a FindValue message with an empty message id, skipping the processing");
                return
            }
            let find_value_reply = match self.store.get(&key) {
                None => {
                    let neighbors = self.routing_table.closest_neighbors(&key_id, 5);
                    let sources: Vec<Source> = neighbors.all_nodes().iter().map(|node| Source::new(node)).collect();
                    Message::find_value_reply_type(message_id.unwrap(), None, Some(sources))
                }
                Some(value) => Message::find_value_reply_type(message_id.unwrap(), Some(value), None),
            };

            let _ = self.async_network.send(find_value_reply, source.endpoint()).await;
        }
    }
}

pub(crate) struct AddNodeAction {
    current_node: Node,
    routing_table: Arc<Table>,
    async_network: Arc<AsyncNetwork>,
}

impl AddNodeAction {
    pub(crate) fn new(
        current_node: Node,
        routing_table: Arc<Table>,
        async_network: Arc<AsyncNetwork>,
    ) -> Self {
        AddNodeAction {
            current_node,
            routing_table,
            async_network,
        }
    }

    async fn send_ping_to(&self, node: &Node, callback: &Arc<ResponseAwaitingCallback>) -> Result<(), NetworkErrorKind> {
        self.async_network
            .send_with_message_id_expect_reply(
                Message::ping_type(self.current_node.clone()),
                &node.endpoint,
                callback.clone()
            )
            .await
    }
}

#[async_trait]
impl MessageAction for AddNodeAction {
    async fn act_on(&self, message: Message) {
        if let AddNode { source } = message {
            let (bucket_index, added) = self.routing_table.add(source.clone().to_node());
            if added {
                return;
            }
            //TODO: add a test to simulate ping reply from the node
            if let Some(node) = self.routing_table.first_node_in(bucket_index) {
                let callback = ResponseAwaitingCallback::new();
                match self.send_ping_to(&node, &callback).await {
                    Ok(_) => {
                        let response_status = callback.handle().await;
                        if let ResponseStatus::Err = response_status {
                            self.routing_table.remove_and_add(
                                bucket_index,
                                &node,
                                source.to_node(),
                            );
                        }
                    }
                    Err(_) => {
                        self.routing_table
                            .remove_and_add(bucket_index, &node, source.to_node())
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod store_message_action_tests {
    use std::sync::Arc;

    use crate::executor::message_action::{MessageAction, StoreMessageAction};
    use crate::id::Id;
    use crate::net::endpoint::Endpoint;
    use crate::net::message::Message;
    use crate::net::node::Node;
    use crate::store::{InMemoryStore, Store};

    #[tokio::test]
    async fn act_on_store_message_and_store_the_key_value_in_store() {
        let store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
        let message_action = StoreMessageAction::new(store.clone());

        let message = Message::store_type(
            "kademlia".as_bytes().to_vec(),
            "distributed hash table".as_bytes().to_vec(),
            Node::new_with_id(
                Endpoint::new("localhost".to_string(), 1909),
                Id::new(511u16.to_be_bytes().to_vec()),
            ),
        );
        message_action.act_on(message).await;

        let value = store.get(&"kademlia".as_bytes().to_vec());
        assert!(value.is_some());

        assert_eq!(
            "distributed hash table",
            String::from_utf8(value.unwrap()).unwrap()
        );
    }
}

#[cfg(test)]
mod ping_message_action_tests {
    use std::sync::Arc;
    use std::time::Duration;

    use tokio::net::TcpListener;

    use crate::executor::message_action::{MessageAction, PingMessageAction};
    use crate::net::AsyncNetwork;
    use crate::net::connection::AsyncTcpConnection;
    use crate::net::endpoint::Endpoint;
    use crate::net::message::Message;
    use crate::net::node::Node;
    use crate::net::wait::{WaitingList, WaitingListOptions};
    use crate::time::SystemClock;

    #[tokio::test]
    async fn send_a_ping_reply() {
        let listener_result = TcpListener::bind("localhost:8009").await;
        assert!(listener_result.is_ok());

        let handle = tokio::spawn(async move {
            let tcp_listener = listener_result.unwrap();
            let stream = tcp_listener.accept().await.unwrap();

            let mut connection = AsyncTcpConnection::new(stream.0);
            let message = connection.read().await.unwrap();

            assert!(message.is_ping_reply_type());
            if let Message::PingReply { to, .. } = message {
                assert_eq!("localhost:7878", to.endpoint().address());
            }
        });

        let async_network = AsyncNetwork::new(waiting_list());
        let current_node = Node::new(Endpoint::new("localhost".to_string(), 7878));
        let message_action = PingMessageAction::new(current_node, async_network);

        let node_sending_ping = Node::new(Endpoint::new("localhost".to_string(), 8009));
        let mut ping_message = Message::ping_type(node_sending_ping);
        ping_message.set_message_id(10) ;

        message_action
            .act_on(ping_message)
            .await;

        handle.await.unwrap();
    }

    fn waiting_list() -> Arc<WaitingList> {
        WaitingList::new(
            WaitingListOptions::new(Duration::from_secs(120), Duration::from_millis(100)),
            SystemClock::new(),
        )
    }
}

#[cfg(test)]
mod add_node_action_tests {
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::net::TcpListener;

    use crate::executor::message_action::{AddNodeAction, MessageAction};
    use crate::id::Id;
    use crate::net::AsyncNetwork;
    use crate::net::endpoint::Endpoint;
    use crate::net::message::Message;
    use crate::net::node::Node;
    use crate::net::wait::{WaitingList, WaitingListOptions};
    use crate::routing::Table;
    use crate::time::SystemClock;

    #[tokio::test]
    async fn act_on_add_node_message_and_add_the_node_in_routing_table() {
        let async_network = AsyncNetwork::new(waiting_list());
        let routing_table: Arc<Table> =
            Arc::new(Table::new(Id::new(255u16.to_be_bytes().to_vec())));

        let message_action = AddNodeAction::new(
            Node::new_with_id(
                Endpoint::new("localhost".to_string(), 1909),
                Id::new(255u16.to_be_bytes().to_vec()),
            ),
            routing_table.clone(),
            async_network
        );

        let message = Message::add_node_type(Node::new_with_id(
            Endpoint::new("localhost".to_string(), 8434),
            Id::new(511u16.to_be_bytes().to_vec()),
        ));
        message_action.act_on(message).await;

        let node = Node::new_with_id(
            Endpoint::new("localhost".to_string(), 8434),
            Id::new(511u16.to_be_bytes().to_vec()),
        );

        let (_, contains) = routing_table.contains(&node);
        assert!(contains);
    }

    #[tokio::test]
    async fn act_on_add_node_message_given_the_bucket_capacity_is_full() {
        let async_network = AsyncNetwork::new(waiting_list());
        let routing_table: Arc<Table> =
            Arc::new(Table::new_with_bucket_capacity(Id::new(255u16.to_be_bytes().to_vec()), 1));

        let message_action = AddNodeAction::new(
            Node::new_with_id(
                Endpoint::new("localhost".to_string(), 1909),
                Id::new(255u16.to_be_bytes().to_vec()),
            ),
            routing_table.clone(),
            async_network
        );

        let message = Message::add_node_type(Node::new_with_id(
            Endpoint::new("localhost".to_string(), 8434),
            Id::new(511u16.to_be_bytes().to_vec()),
        ));
        message_action.act_on(message).await;

        let message = Message::add_node_type(Node::new_with_id(
            Endpoint::new("localhost".to_string(), 7878),
            Id::new(511u16.to_be_bytes().to_vec()),
        ));
        message_action.act_on(message).await;

        let node = Node::new_with_id(
            Endpoint::new("localhost".to_string(), 7878),
            Id::new(511u16.to_be_bytes().to_vec()),
        );

        let (_, contains) = routing_table.contains(&node);
        assert!(contains);

        let node = Node::new_with_id(
            Endpoint::new("localhost".to_string(), 8434),
            Id::new(511u16.to_be_bytes().to_vec()),
        );

        let (_, contains) = routing_table.contains(&node);
        assert_eq!(false, contains);
    }

    #[tokio::test]
    async fn act_on_add_node_message_given_the_bucket_capacity_is_full_and_the_node_to_ping_does_not_reply() {
        let listener_result = TcpListener::bind("localhost:8436").await;
        assert!(listener_result.is_ok());

        let waiting_list = WaitingList::new(
            WaitingListOptions::new(Duration::from_millis(120), Duration::from_millis(30)),
            SystemClock::new(),
        );

        let async_network = AsyncNetwork::new(waiting_list);
        let routing_table: Arc<Table> =
            Arc::new(Table::new_with_bucket_capacity(Id::new(255u16.to_be_bytes().to_vec()), 1));

        let message_action = AddNodeAction::new(
            Node::new_with_id(
                Endpoint::new("localhost".to_string(), 1909),
                Id::new(255u16.to_be_bytes().to_vec()),
            ),
            routing_table.clone(),
            async_network
        );

        let message = Message::add_node_type(Node::new_with_id(
            Endpoint::new("localhost".to_string(), 8436),
            Id::new(511u16.to_be_bytes().to_vec()),
        ));
        message_action.act_on(message).await;

        let message = Message::add_node_type(Node::new_with_id(
            Endpoint::new("localhost".to_string(), 7880),
            Id::new(511u16.to_be_bytes().to_vec()),
        ));
        message_action.act_on(message).await;

        let node = Node::new_with_id(
            Endpoint::new("localhost".to_string(), 7880),
            Id::new(511u16.to_be_bytes().to_vec()),
        );

        let (_, contains) = routing_table.contains(&node);
        assert!(contains);

        let node = Node::new_with_id(
            Endpoint::new("localhost".to_string(), 8436),
            Id::new(511u16.to_be_bytes().to_vec()),
        );

        let (_, contains) = routing_table.contains(&node);
        assert_eq!(false, contains);
    }

    fn waiting_list() -> Arc<WaitingList> {
        WaitingList::new(
            WaitingListOptions::new(Duration::from_secs(120), Duration::from_millis(100)),
            SystemClock::new(),
        )
    }
}

#[cfg(test)]
mod find_value_message_action_tests {
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::net::TcpListener;

    use crate::executor::message_action::{FindValueMessageAction, MessageAction};
    use crate::id::Id;
    use crate::net::AsyncNetwork;
    use crate::net::connection::AsyncTcpConnection;
    use crate::net::endpoint::Endpoint;
    use crate::net::message::Message;
    use crate::net::node::Node;
    use crate::net::wait::{WaitingList, WaitingListOptions};
    use crate::routing::Table;
    use crate::store::{InMemoryStore, Key, Store};
    use crate::time::SystemClock;

    #[tokio::test]
    async fn act_on_find_value_message_given_value_for_the_key_is_found_in_store() {
        let listener_result = TcpListener::bind("localhost:8712").await;
        assert!(listener_result.is_ok());

        let handle = tokio::spawn(async move {
            let tcp_listener = listener_result.unwrap();
            let stream = tcp_listener.accept().await.unwrap();

            let mut connection = AsyncTcpConnection::new(stream.0);
            let message = connection.read().await.unwrap();

            assert!(message.is_find_value_reply_type());
            if let Message::FindValueReply { message_id, value, .. } = message {
                assert_eq!(100, message_id);
                assert_eq!("distributed hash table".as_bytes().to_vec(), value.unwrap());
            }
        });

        let async_network = AsyncNetwork::new(waiting_list());
        let routing_table: Arc<Table> =
            Arc::new(Table::new(Id::new(255u16.to_be_bytes().to_vec())));

        let store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
        let message_action = FindValueMessageAction::new(store.clone(), routing_table, async_network);

        store.put_or_update(Key::new("kademlia".as_bytes().to_vec()), "distributed hash table".as_bytes().to_vec());

        let mut message = Message::find_value_type(
            Node::new_with_id(
                Endpoint::new("localhost".to_string(), 8712),
                Id::new(511u16.to_be_bytes().to_vec()),
            ),
            "kademlia".as_bytes().to_vec()
        );
        message.set_message_id(100);

        message_action.act_on(message).await;

        handle.await.unwrap();
    }

    #[tokio::test]
    async fn act_on_find_value_message_given_value_for_the_key_is_not_found_in_store() {
        let listener_result = TcpListener::bind("localhost:9912").await;
        assert!(listener_result.is_ok());

        let handle = tokio::spawn(async move {
            let tcp_listener = listener_result.unwrap();
            let stream = tcp_listener.accept().await.unwrap();

            let mut connection = AsyncTcpConnection::new(stream.0);
            let message = connection.read().await.unwrap();

            assert!(message.is_find_value_reply_type());
            if let Message::FindValueReply { message_id, value: _, neighbors, } = message {
                assert_eq!(100, message_id);

                let neighbors = neighbors.unwrap();
                assert_eq!(2, neighbors.len());
                assert_eq!(&Id::new(247u16.to_be_bytes().to_vec()), neighbors.get(0).unwrap().node_id());
                assert_eq!(&Id::new(249u16.to_be_bytes().to_vec()), neighbors.get(1).unwrap().node_id());
            }
        });

        let async_network = AsyncNetwork::new(waiting_list());
        let routing_table: Arc<Table> =
            Arc::new(Table::new(Id::new(255u16.to_be_bytes().to_vec())));

        let store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
        let message_action = FindValueMessageAction::new(store, routing_table.clone(), async_network);

        routing_table.add(
            Node::new_with_id(
                Endpoint::new("localhost".to_string(), 7070),
                Id::new(247u16.to_be_bytes().to_vec()),
            )
        );
        routing_table.add(
            Node::new_with_id(
                Endpoint::new("localhost".to_string(), 8989),
                Id::new(249u16.to_be_bytes().to_vec()),
            )
        );

        let mut message = Message::find_value_type(
            Node::new_with_id(
                Endpoint::new("localhost".to_string(), 9912),
                Id::new(511u16.to_be_bytes().to_vec()),
            ),
            "kademlia".as_bytes().to_vec()
        );
        message.set_message_id(100);

        message_action.act_on(message).await;

        handle.await.unwrap();
    }

    fn waiting_list() -> Arc<WaitingList> {
        WaitingList::new(
            WaitingListOptions::new(Duration::from_secs(120), Duration::from_millis(100)),
            SystemClock::new(),
        )
    }
}
