use std::mem::size_of;

use bincode;
use serde::Deserialize;
use serde::Serialize;

use crate::net::endpoint::Endpoint;
use crate::net::node::{Node, NodeId};
use crate::store::KeyId;

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct Source {
    node_endpoint: Endpoint,
    node_id: NodeId,
}

impl Source {
    pub(crate) fn to_node(self) -> Node {
        Node::new_with_id(self.node_endpoint, self.node_id)
    }
}

const U32_SIZE: usize = size_of::<u32>();

#[derive(Serialize, Deserialize, Clone)]
pub(crate) enum Message {
    Store {
        key: Vec<u8>,
        key_id: KeyId,
        value: Vec<u8>,
        source: Source,
    },
    FindValue {
        key: Vec<u8>,
        key_id: KeyId,
    },
    FindNode {
        node_id: NodeId,
    },
    ShutDown,
}

impl Message {
    pub(crate) fn store_type(key: Vec<u8>, value: Vec<u8>, source: Node) -> Self {
        let key_id = KeyId::generate_from_bytes(&key);
        Message::Store {
            key,
            key_id,
            value,
            source: Source {
                node_endpoint: source.endpoint,
                node_id: source.id,
            },
        }
    }

    pub(crate) fn find_value_type(key: Vec<u8>) -> Self {
        let key_id = KeyId::generate_from_bytes(&key);
        Message::FindValue { key, key_id }
    }

    pub(crate) fn find_node_type(node_id: NodeId) -> Self {
        Message::FindNode { node_id }
    }

    pub(crate) fn shutdown_type() -> Self {
        Message::ShutDown
    }

    pub(crate) fn deserialize_from(bytes: &[u8]) -> bincode::Result<Message> {
        bincode::deserialize(&bytes[U32_SIZE..])
    }

    pub(crate) fn serialize(&self) -> bincode::Result<Vec<u8>> {
        let result = bincode::serialize(self);
        result.map(|mut bytes| {
            let size: u32 = bytes.len() as u32;
            let mut size = size.to_be_bytes().to_vec();

            let mut serialized = Vec::new();
            serialized.append(&mut size);
            serialized.append(&mut bytes);

            serialized
        })
    }

    fn is_store_type(&self) -> bool {
        if let Message::Store { .. } = self {
            return true;
        }
        return false;
    }

    fn is_find_value_type(&self) -> bool {
        if let Message::FindValue { .. } = self {
            return true;
        }
        return false;
    }

    fn is_find_node_type(&self) -> bool {
        if let Message::FindNode { .. } = self {
            return true;
        }
        return false;
    }
}

#[cfg(test)]
mod tests {
    use crate::id::{Id, EXPECTED_ID_LENGTH_IN_BYTES};
    use crate::net::endpoint::Endpoint;
    use crate::net::message::Message;
    use crate::net::node::{Node, NodeId};

    #[test]
    fn serialize_deserialize_a_store_message() {
        let store_type = Message::store_type(
            "kademlia".as_bytes().to_vec(),
            "distributed hash table".as_bytes().to_vec(),
            Node::new_with_id(
                Endpoint::new("localhost".to_string(), 1010),
                Id::new(vec![10, 20]),
            ),
        );
        let serialized = store_type.serialize().unwrap();
        let deserialized = Message::deserialize_from(&serialized).unwrap();

        assert!(deserialized.is_store_type());
        match deserialized {
            Message::Store {
                key,
                key_id: _,
                value,
                source,
            } => {
                assert_eq!("kademlia", String::from_utf8(key).unwrap());
                assert_eq!("distributed hash table", String::from_utf8(value).unwrap());
                assert_eq!(Id::new(vec![10, 20]), source.node_id);
            }
            _ => {
                panic!("Expected store type message, but was not");
            }
        }
    }

    #[test]
    fn serialize_deserialize_a_find_value_message() {
        let store_type = Message::find_value_type("kademlia".as_bytes().to_vec());
        let serialized = store_type.serialize().unwrap();
        let deserialized = Message::deserialize_from(&serialized).unwrap();

        assert!(deserialized.is_find_value_type());
        match deserialized {
            Message::FindValue { key, key_id: _ } => {
                assert_eq!("kademlia", String::from_utf8(key).unwrap())
            }
            _ => {
                panic!("Expected findValue type message, but was not");
            }
        }
    }

    #[test]
    fn serialize_deserialize_a_find_node_message() {
        let store_type =
            Message::find_node_type(NodeId::generate_from("localhost:8989".to_string()));
        let serialized = store_type.serialize().unwrap();
        let deserialized = Message::deserialize_from(&serialized).unwrap();

        assert!(deserialized.is_find_node_type());
        match deserialized {
            Message::FindNode { node_id } => {
                assert_eq!(EXPECTED_ID_LENGTH_IN_BYTES, node_id.len())
            }
            _ => {
                panic!("Expected findNode type message, but was not");
            }
        }
    }
}
