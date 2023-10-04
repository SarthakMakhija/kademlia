use std::mem::size_of;

use bincode;
use serde::Deserialize;
use serde::Serialize;

use crate::net::endpoint::Endpoint;
use crate::net::message::Message::{FindValue, Ping, SendPingReply};
use crate::net::node::{Node, NodeId};
use crate::store::KeyId;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub(crate) struct Source {
    node_endpoint: Endpoint,
    node_id: NodeId,
}

impl Source {
    pub(crate) fn to_node(self) -> Node {
        Node::new_with_id(self.node_endpoint, self.node_id)
    }

    pub(crate) fn endpoint(&self) -> &Endpoint {
        &self.node_endpoint
    }
}

pub(crate) const U32_SIZE: usize = size_of::<u32>();

pub(crate) type MessageId = i64;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub(crate) enum Message {
    Store {
        key: Vec<u8>,
        key_id: KeyId,
        value: Vec<u8>,
        source: Source,
    },
    FindValue {
        message_id: Option<MessageId>,
        key: Vec<u8>,
        key_id: KeyId,
    },
    FindNode {
        message_id: Option<MessageId>,
        node_id: NodeId,
    },
    Ping {
        message_id: Option<MessageId>,
        from: Source,
    },
    SendPingReply {
        message_id: Option<MessageId>,
        to: Source,
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
        FindValue {
            message_id: None,
            key,
            key_id,
        }
    }

    pub(crate) fn find_node_type(node_id: NodeId) -> Self {
        Message::FindNode {
            message_id: None,
            node_id,
        }
    }

    pub(crate) fn ping_type(current_node: Node) -> Self {
        Ping {
            message_id: None,
            from: Source {
                node_endpoint: current_node.endpoint,
                node_id: current_node.id,
            },
        }
    }

    pub(crate) fn ping_reply_type(current_node: Node, message_id: Option<MessageId>) -> Self {
        SendPingReply {
            message_id,
            to: Source {
                node_endpoint: current_node.endpoint,
                node_id: current_node.id,
            },
        }
    }

    pub(crate) fn shutdown_type() -> Self {
        Message::ShutDown
    }

    pub(crate) fn is_find_value_type(&self) -> bool {
        if let FindValue { .. } = self {
            return true;
        }
        return false;
    }

    pub(crate) fn is_ping_reply_type(&self) -> bool {
        if let SendPingReply { .. } = self {
            return true;
        }
        return false;
    }

    pub(crate) fn is_shutdown_type(&self) -> bool {
        if let Message::ShutDown = self {
            return true;
        }
        return false;
    }

    pub(crate) fn is_ping_type(&self) -> bool {
        if let Ping { .. } = self {
            return true;
        }
        return false;
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

    pub(crate) fn set_message_id(&mut self, id: MessageId) {
        match self {
            FindValue { message_id, .. }
            | Message::FindNode { message_id, .. }
            | Ping { message_id, .. } => *message_id = Some(id),
            Message::Store { .. } => {}
            SendPingReply { .. } => {}
            Message::ShutDown => {}
        }
    }

    fn is_store_type(&self) -> bool {
        if let Message::Store { .. } = self {
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
        let find_value_type = Message::find_value_type("kademlia".as_bytes().to_vec());
        let serialized = find_value_type.serialize().unwrap();
        let deserialized = Message::deserialize_from(&serialized).unwrap();

        assert!(deserialized.is_find_value_type());
        match deserialized {
            Message::FindValue {
                message_id: _,
                key,
                key_id: _,
            } => {
                assert_eq!("kademlia", String::from_utf8(key).unwrap())
            }
            _ => {
                panic!("Expected findValue type message, but was not");
            }
        }
    }

    #[test]
    fn serialize_deserialize_a_find_value_message_with_message_id() {
        let mut find_value_type = Message::find_value_type("kademlia".as_bytes().to_vec());
        find_value_type.set_message_id(10);

        let serialized = find_value_type.serialize().unwrap();
        let deserialized = Message::deserialize_from(&serialized).unwrap();

        assert!(deserialized.is_find_value_type());
        match deserialized {
            Message::FindValue {
                message_id,
                key,
                key_id: _,
            } => {
                assert_eq!("kademlia", String::from_utf8(key).unwrap());
                assert_eq!(Some(10), message_id);
            }
            _ => {
                panic!("Expected findValue type message, but was not");
            }
        }
    }

    #[test]
    fn serialize_deserialize_a_find_node_message() {
        let find_node_type =
            Message::find_node_type(NodeId::generate_from("localhost:8989".to_string()));
        let serialized = find_node_type.serialize().unwrap();
        let deserialized = Message::deserialize_from(&serialized).unwrap();

        assert!(deserialized.is_find_node_type());
        match deserialized {
            Message::FindNode {
                message_id: _,
                node_id,
            } => {
                assert_eq!(EXPECTED_ID_LENGTH_IN_BYTES, node_id.len())
            }
            _ => {
                panic!("Expected findNode type message, but was not");
            }
        }
    }

    #[test]
    fn set_message_id_in_find_value() {
        let mut find_value_type = Message::find_value_type("kademlia".as_bytes().to_vec());
        find_value_type.set_message_id(100);

        assert!(find_value_type.is_find_value_type());
        if let Message::FindValue { message_id, .. } = find_value_type {
            assert_eq!(Some(100), message_id);
        }
    }

    #[test]
    fn set_message_id_in_find_node() {
        let mut find_node_type =
            Message::find_node_type(NodeId::generate_from("localhost:8989".to_string()));
        find_node_type.set_message_id(100);

        assert!(find_node_type.is_find_node_type());
        if let Message::FindNode { message_id, .. } = find_node_type {
            assert_eq!(Some(100), message_id);
        }
    }

    #[test]
    fn set_message_id_in_ping() {
        let mut ping_type =
            Message::ping_type(Node::new(Endpoint::new("localhost".to_string(), 2334)));
        ping_type.set_message_id(100);

        assert!(ping_type.is_ping_type());
        if let Message::Ping { message_id, .. } = ping_type {
            assert_eq!(Some(100), message_id);
        }
    }
}
