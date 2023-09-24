use num_bigint::{BigInt, Sign};
use crate::net::endpoint::Endpoint;

const EXPECTED_NODE_ID_LENGTH: usize = 20;

pub(crate) type NodeId = Vec<u8>;

#[derive(Eq, PartialEq)]
pub(crate) struct Node {
    id: NodeId,
    endpoint: Endpoint
}

impl Node {
    fn new(id: Vec<u8>, endpoint: Endpoint) -> Self {
        if id.len() != EXPECTED_NODE_ID_LENGTH {
            let error_message = format!("node id length must be {}, but was {}", EXPECTED_NODE_ID_LENGTH, id.len());
            panic!("{}", error_message.as_str())
        }
        Node {
            id,
            endpoint
        }
    }

    fn distance_from(&self, other: &Node) -> BigInt {
        let distance: Vec<u8> = self.id.iter()
            .zip(other.id.iter())
            .map(|(&first_node_id,&other_node_id)| first_node_id ^ other_node_id)
            .collect();

        BigInt::from_bytes_be(Sign::Plus, &distance)
    }
}

#[cfg(test)]
mod tests {
    use num_bigint::{BigInt, Sign};
    use crate::net::endpoint::Endpoint;
    use crate::node::{EXPECTED_NODE_ID_LENGTH, Node};

    #[test]
    #[should_panic]
    fn incorrect_node_id_length() {
        let _node = Node::new(
            vec![10],
            Endpoint::new("localhost".to_string(), 2330)
        );
    }

    #[test]
    fn node_equals_itself() {
        let node = Node::new(
            vec![0; 20],
            Endpoint::new("localhost".to_string(), 2330)
        );
        assert!(node.eq(&node))
    }

    #[test]
    fn node_equals_other_node() {
        let node = Node::new(
            vec![0; EXPECTED_NODE_ID_LENGTH],
            Endpoint::new("localhost".to_string(), 2330)
        );
        let other_node = Node::new(
            vec![0; EXPECTED_NODE_ID_LENGTH],
            Endpoint::new("localhost".to_string(), 2330)
        );
        assert!(node.eq(&other_node))
    }

    #[test]
    fn node_does_not_equal_other_node() {
        let node = Node::new(
            vec![0; EXPECTED_NODE_ID_LENGTH],
            Endpoint::new("localhost".to_string(), 2330)
        );
        let other_node = Node::new(
            vec![1; EXPECTED_NODE_ID_LENGTH],
            Endpoint::new("localhost".to_string(), 1982)
        );
        assert!(node.ne(&other_node))
    }

    #[test]
    fn distance_from_other_node() {
        let node = Node::new(
            vec![0; EXPECTED_NODE_ID_LENGTH],
            Endpoint::new("localhost".to_string(), 2330)
        );
        let other_node = Node::new(
            vec![1; EXPECTED_NODE_ID_LENGTH],
            Endpoint::new("localhost".to_string(), 1982)
        );

        let expected_distance = BigInt::from_bytes_be(Sign::Plus, &vec![1; EXPECTED_NODE_ID_LENGTH]);
        let distance = node.distance_from(&other_node);
        assert_eq!(expected_distance, distance);
    }
}