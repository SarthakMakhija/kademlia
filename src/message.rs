use bincode;
use serde::Deserialize;
use serde::Serialize;

use crate::net::node::NodeId;
use crate::store::KeyId;

#[derive(Serialize, Deserialize)]
enum Messages {
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
        node_id: NodeId
    },
}

impl Messages {
    fn store_type(key: Vec<u8>, value: Vec<u8>) -> Messages {
        let key_id = KeyId::generate_from_bytes(&key);
        Messages::Store {
            key,
            key_id,
            value,
        }
    }

    fn find_value_type(key: Vec<u8>) -> Messages {
        let key_id = KeyId::generate_from_bytes(&key);
        Messages::FindValue {
            key,
            key_id,
        }
    }

    fn find_node_type(node_id: NodeId) -> Messages {
        Messages::FindNode {
            node_id
        }
    }

    fn deserialize_from(bytes: &[u8]) -> bincode::Result<Messages> {
        bincode::deserialize(bytes)
    }

    fn serialize(&self) -> bincode::Result<Vec<u8>> {
        bincode::serialize(self)
    }

    fn is_store_type(&self) -> bool {
        if let Messages::Store { .. } = self {
            return true;
        }
        return false;
    }

    fn is_find_value_type(&self) -> bool {
        if let Messages::FindValue { .. } = self {
            return true;
        }
        return false;
    }

    fn is_find_node_type(&self) -> bool {
        if let Messages::FindNode { .. } = self {
            return true;
        }
        return false;
    }
}

#[cfg(test)]
mod tests {
    use crate::id::EXPECTED_ID_LENGTH_IN_BYTES;
    use crate::message::Messages;
    use crate::net::node::NodeId;

    #[test]
    fn serialize_deserialize_a_store_message() {
        let store_type = Messages::store_type(
            "kademlia".as_bytes().to_vec(),
            "distributed hash table".as_bytes().to_vec(),
        );
        let serialized = store_type.serialize().unwrap();
        let deserialized = Messages::deserialize_from(&serialized).unwrap();

        assert!(deserialized.is_store_type());
        match deserialized {
            Messages::Store { key, key_id: _, value } => {
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
        let store_type = Messages::find_value_type(
            "kademlia".as_bytes().to_vec(),
        );
        let serialized = store_type.serialize().unwrap();
        let deserialized = Messages::deserialize_from(&serialized).unwrap();

        assert!(deserialized.is_find_value_type());
        match deserialized {
            Messages::FindValue { key, key_id: _ } => assert_eq!("kademlia", String::from_utf8(key).unwrap()),
            _ => {
                panic!("Expected findValue type message, but was not");
            }
        }
    }

    #[test]
    fn serialize_deserialize_a_find_node_message() {
        let store_type = Messages::find_node_type(
            NodeId::generate_from("localhost:8989".to_string())
        );
        let serialized = store_type.serialize().unwrap();
        let deserialized = Messages::deserialize_from(&serialized).unwrap();

        assert!(deserialized.is_find_node_type());
        match deserialized {
            Messages::FindNode { node_id } => assert_eq!(EXPECTED_ID_LENGTH_IN_BYTES, node_id.len()),
            _ => {
                panic!("Expected findNode type message, but was not");
            }
        }
    }
}