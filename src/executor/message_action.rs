use std::sync::Arc;

use crate::net::AsyncNetwork;
use crate::net::message::Message;
use crate::net::message::Message::AddNode;
use crate::net::node::Node;
use crate::routing::Table;
use crate::store::{Key, Store};

pub(crate) trait MessageAction: Send + Sync {
    fn act_on(&self, message: Message);
}

pub(crate) struct StoreMessageAction {
    store: Arc<dyn Store>
}

impl StoreMessageAction {
    pub(crate) fn new(store:  Arc<dyn Store>) -> Self {
        StoreMessageAction {
            store,
        }
    }
}

impl MessageAction for StoreMessageAction {
    fn act_on(&self, message: Message) {
        if let Message::Store {
            key,
            key_id,
            value,
            ..
        } = message {
            self.store
                .put_or_update(Key::new_with_id(key, key_id), value);
        }
    }
}

pub(crate) struct PingMessageAction {
    current_node:  Node,
    async_network:  Arc<AsyncNetwork>,
}

impl PingMessageAction {
    pub(crate) fn new(
        current_node:  Node,
        async_network:  Arc<AsyncNetwork>,
    ) -> Self {
        PingMessageAction {
            current_node,
            async_network,
        }
    }
}

impl MessageAction for PingMessageAction {
    fn act_on(&self, message: Message) {
        if let Message::Ping { message_id, from } = message {
            let current_node = self.current_node.clone();
            let async_network = self.async_network.clone();

            tokio::spawn(async move {
                let _ = async_network
                    .send(
                        Message::ping_reply_type(current_node, message_id),
                        from.endpoint(),
                    )
                    .await;
            });
        }
    }
}

pub(crate) struct AddNodeAction {
    routing_table:  Arc<Table>,
}

impl AddNodeAction {
    pub(crate) fn new(routing_table:  Arc<Table>) -> Self {
        AddNodeAction {
            routing_table,
        }
    }
}

impl MessageAction for AddNodeAction {
    fn act_on(&self, message: Message) {
        if let AddNode { source, } = message {
            self.routing_table.add(source.to_node());
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

    #[test]
    fn act_on_store_message_and_store_the_key_value_in_store() {
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
        message_action.act_on(message);

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
            if let Message::SendPingReply { to, .. } = message {
                assert_eq!("localhost:7878", to.endpoint().address());
            }
        });

        let async_network = AsyncNetwork::new(waiting_list());
        let current_node = Node::new(Endpoint::new("localhost".to_string(), 7878));
        let message_action = PingMessageAction::new(current_node, async_network);

        let node_sending_ping = Node::new(Endpoint::new("localhost".to_string(), 8009));
        message_action.act_on(Message::ping_type(node_sending_ping));

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

    use crate::executor::message_action::{AddNodeAction, MessageAction};
    use crate::id::Id;
    use crate::net::endpoint::Endpoint;
    use crate::net::message::Message;
    use crate::net::node::Node;
    use crate::routing::Table;

    #[test]
    fn act_on_store_message_and_add_the_node_in_routing_table() {
        let routing_table: Arc<Table> =
            Arc::new(Table::new(Id::new(255u16.to_be_bytes().to_vec())));

        let message_action = AddNodeAction::new(routing_table.clone());

        let message = Message::add_node_type(
            Node::new_with_id(
                Endpoint::new("localhost".to_string(), 1909),
                Id::new(511u16.to_be_bytes().to_vec()),
            ),
        );
        message_action.act_on(message);

        let node = Node::new_with_id(
            Endpoint::new("localhost".to_string(), 1909),
            Id::new(511u16.to_be_bytes().to_vec()),
        );

        let (_, contains) = routing_table.contains(&node);
        assert!(contains);
    }
}