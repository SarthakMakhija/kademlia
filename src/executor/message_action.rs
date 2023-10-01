use std::sync::Arc;

use crate::message::Message;
use crate::store::{Key, Store};

pub(crate) trait MessageAction {
    fn act_on(&self, message: Message);
}

pub(crate) struct StoreMessageAction<'a> {
    store: &'a Arc<dyn Store>,
}

impl<'a> StoreMessageAction<'a> {
    pub(crate) fn new(store: &'a Arc<dyn Store>) -> Self {
        StoreMessageAction { store }
    }
}

impl<'a> MessageAction for StoreMessageAction<'a> {
    fn act_on(&self, message: Message) {
        match message {
            Message::Store {
                key,
                key_id,
                value,
                source: _source,
            } => {
                self.store
                    .put_or_update(Key::new_with_id(key, key_id), value);
            }
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::executor::message_action::{MessageAction, StoreMessageAction};
    use crate::message::Message;
    use crate::net::endpoint::Endpoint;
    use crate::net::node::Node;
    use crate::store::{InMemoryStore, Store};

    #[test]
    fn ok() {
        let store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
        let message_action = StoreMessageAction::new(&store);

        let message = Message::store_type(
            "kademlia".as_bytes().to_vec(),
            "distributed hash table".as_bytes().to_vec(),
            Node::new(Endpoint::new("localhost".to_string(), 1909)),
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
