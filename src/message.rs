use bincode;
use serde::Deserialize;
use serde::Serialize;

use crate::net::node::NodeId;
use crate::store::KeyId;

#[derive(Serialize, Deserialize)]
pub(crate) enum Message {
    Store {
        key: Vec<u8>,
        key_id: KeyId,
        value: Vec<u8>,
    },
    FindValue {
        key: Vec<u8>,
        key_id: KeyId,
    },
    FindNode {
        node_id: NodeId,
    },
}

impl Message {
    pub(crate) fn store_type(key: Vec<u8>, value: Vec<u8>) -> Message {
        let key_id = KeyId::generate_from_bytes(&key);
        Message::Store { key, key_id, value }
    }

    pub(crate) fn find_value_type(key: Vec<u8>) -> Message {
        let key_id = KeyId::generate_from_bytes(&key);
        Message::FindValue { key, key_id }
    }

    pub(crate) fn find_node_type(node_id: NodeId) -> Message {
        Message::FindNode { node_id }
    }

    pub(crate) fn deserialize_from(bytes: &[u8]) -> bincode::Result<Message> {
        bincode::deserialize(bytes)
    }

    pub(crate) fn serialize(&self) -> bincode::Result<Vec<u8>> {
        bincode::serialize(self)
    }

    pub(crate) fn is_store_type(&self) -> bool {
        if let Message::Store { .. } = self {
            return true;
        }
        return false;
    }

    pub(crate) fn is_find_value_type(&self) -> bool {
        if let Message::FindValue { .. } = self {
            return true;
        }
        return false;
    }

    pub(crate) fn is_find_node_type(&self) -> bool {
        if let Message::FindNode { .. } = self {
            return true;
        }
        return false;
    }
}

#[cfg(test)]
mod tests {
    use crate::id::EXPECTED_ID_LENGTH_IN_BYTES;
    use crate::message::Message;
    use crate::net::node::NodeId;

    #[test]
    fn serialize_deserialize_a_store_message() {
        let store_type = Message::store_type(
            "kademlia".as_bytes().to_vec(),
            "distributed hash table".as_bytes().to_vec(),
        );
        let serialized = store_type.serialize().unwrap();
        let deserialized = Message::deserialize_from(&serialized).unwrap();

        assert!(deserialized.is_store_type());
        match deserialized {
            Message::Store {
                key,
                key_id: _,
                value,
            } => {
                assert_eq!("kademlia", String::from_utf8(key).unwrap());
                assert_eq!("distributed hash table", String::from_utf8(value).unwrap());
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
