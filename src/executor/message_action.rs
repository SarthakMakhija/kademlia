use std::sync::Arc;

use crate::net::message::Message;
use crate::routing::Table;
use crate::store::{Key, Store};

pub(crate) trait MessageAction {
    fn act_on(&self, message: Message);
}

pub(crate) struct StoreMessageAction<'action> {
    store: &'action Arc<dyn Store>,
    routing_table: &'action Arc<Table>,
}

impl<'action> StoreMessageAction<'action> {
    pub(crate) fn new(store: &'action Arc<dyn Store>, routing_table: &'action Arc<Table>) -> Self {
        StoreMessageAction {
            store,
            routing_table,
        }
    }
}

impl<'action> MessageAction for StoreMessageAction<'action> {
    fn act_on(&self, message: Message) {
        match message {
            Message::Store {
                key,
                key_id,
                value,
                source,
            } => {
                self.store
                    .put_or_update(Key::new_with_id(key, key_id), value);
                self.routing_table.add(source.to_node());
            }
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::executor::message_action::{MessageAction, StoreMessageAction};
    use crate::id::Id;
    use crate::net::endpoint::Endpoint;
    use crate::net::message::Message;
    use crate::net::node::Node;
    use crate::routing::Table;
    use crate::store::{InMemoryStore, Store};

    #[test]
    fn act_on_store_message_and_store_the_key_value_in_store() {
        let store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
        let routing_table: Arc<Table> =
            Arc::new(Table::new(Id::new(255u16.to_be_bytes().to_vec())));

        let message_action = StoreMessageAction::new(&store, &routing_table);

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

        let node = Node::new_with_id(
            Endpoint::new("localhost".to_string(), 1909),
            Id::new(511u16.to_be_bytes().to_vec()),
        );

        let (_, contains) = routing_table.contains(&node);
        assert!(contains);
    }

    #[test]
    fn act_on_store_message_and_add_the_node_in_routing_table() {
        let store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
        let routing_table: Arc<Table> =
            Arc::new(Table::new(Id::new(255u16.to_be_bytes().to_vec())));

        let message_action = StoreMessageAction::new(&store, &routing_table);

        let message = Message::store_type(
            "kademlia".as_bytes().to_vec(),
            "distributed hash table".as_bytes().to_vec(),
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
