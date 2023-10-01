use std::sync::mpsc::{Receiver, SendError, Sender};
use std::sync::{mpsc, Arc};
use std::thread;

use crate::executor::response::{ChanneledMessage, MessageResponse, MessageStatus};
use crate::message::Message;

use crate::store::{Key, Store};

mod response;

pub(crate) struct MessageExecutor {
    sender: Sender<ChanneledMessage>,
}

impl MessageExecutor {
    pub(crate) fn new(store: Arc<dyn Store>) -> Self {
        let (sender, receiver) = mpsc::channel();
        let executor = MessageExecutor { sender };
        executor.start(receiver, store);
        executor
    }

    pub(crate) fn submit(
        &self,
        message: Message,
    ) -> Result<MessageResponse, SendError<ChanneledMessage>> {
        let (sender, receiver) = mpsc::channel();
        self.sender
            .send(ChanneledMessage::new(message, sender))
            .map(|_| MessageResponse::new(receiver))
    }

    pub(crate) fn shutdown(&self) -> Result<MessageResponse, SendError<ChanneledMessage>> {
        self.submit(Message::shutdown_type())
    }

    fn start(&self, receiver: Receiver<ChanneledMessage>, store: Arc<dyn Store>) {
        thread::spawn(move || loop {
            match receiver.recv() {
                Ok(channeled_message) => match channeled_message.message.clone() {
                    Message::Store {
                        key,
                        key_id,
                        value,
                        source: _source,
                    } => {
                        store.put_or_update(Key::new_with_id(key, key_id), value);
                        let _ = channeled_message.send_response(MessageStatus::StoreDone);
                    }
                    Message::ShutDown => {
                        let _ = channeled_message.send_response(MessageStatus::ShutdownDone);
                        return;
                    }
                    _ => {}
                },
                Err(err) => {
                    println!("received an err {:?}", err);
                    return;
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::executor::MessageExecutor;
    use crate::message::Message;
    use crate::net::endpoint::Endpoint;
    use crate::net::node::Node;
    use crate::store::{InMemoryStore, Store};

    #[test]
    fn submit_store_message_successfully() {
        let store = Arc::new(InMemoryStore::new());
        let executor = MessageExecutor::new(store.clone());

        let submit_result = executor.submit(Message::store_type(
            "kademlia".as_bytes().to_vec(),
            "distributed hash table".as_bytes().to_vec(),
            Node::new(Endpoint::new("localhost".to_string(), 1909)),
        ));
        assert!(submit_result.is_ok());
    }

    #[test]
    fn submit_store_message_with_successful_message_store() {
        let store = Arc::new(InMemoryStore::new());
        let executor = MessageExecutor::new(store.clone());

        let submit_result = executor.submit(Message::store_type(
            "kademlia".as_bytes().to_vec(),
            "distributed hash table".as_bytes().to_vec(),
            Node::new(Endpoint::new("localhost".to_string(), 1909)),
        ));
        assert!(submit_result.is_ok());

        let message_response = submit_result.unwrap();
        let message_response_result = message_response.wait_until_response_is_received();
        assert!(message_response_result.is_ok());

        let message_status = message_response_result.unwrap();
        assert!(message_status.is_store_done());
    }

    #[test]
    fn submit_store_message_with_successful_value_in_store() {
        let store = Arc::new(InMemoryStore::new());
        let executor = MessageExecutor::new(store.clone());

        let submit_result = executor.submit(Message::store_type(
            "kademlia".as_bytes().to_vec(),
            "distributed hash table".as_bytes().to_vec(),
            Node::new(Endpoint::new("localhost".to_string(), 1909)),
        ));
        assert!(submit_result.is_ok());

        let message_response = submit_result.unwrap();
        let message_response_result = message_response.wait_until_response_is_received();
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

    #[test]
    fn submit_a_message_after_shutdown() {
        let store = Arc::new(InMemoryStore::new());
        let executor = MessageExecutor::new(store.clone());

        let submit_result = executor.shutdown();
        assert!(submit_result.is_ok());

        let message_response = submit_result.unwrap();
        let message_response_result = message_response.wait_until_response_is_received();
        assert!(message_response_result.is_ok());

        let submit_result = executor.submit(Message::store_type(
            "kademlia".as_bytes().to_vec(),
            "distributed hash table".as_bytes().to_vec(),
            Node::new(Endpoint::new("localhost".to_string(), 1909)),
        ));
        assert!(submit_result.is_err());
    }
}
