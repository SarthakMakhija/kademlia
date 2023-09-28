use num_bigint::{BigInt};

use crate::id::Id;
use crate::net::endpoint::Endpoint;

pub(crate) type NodeId = Id;

#[derive(Eq, PartialEq, Clone)]
pub(crate) struct Node {
    pub(crate) id: NodeId,
    endpoint: Endpoint,
}

impl Node {
    pub(crate) fn new(endpoint: Endpoint) -> Self {
        Node {
            id: Id::generate_from(endpoint.address()),
            endpoint,
        }
    }

    #[cfg(test)]
    pub(crate) fn new_with_id(endpoint: Endpoint, id: Id) -> Self {
        Node {
            id,
            endpoint,
        }
    }

    fn distance_from(&self, other: &Node) -> BigInt {
        self.id.distance_from(&other.id)
    }
}

#[cfg(test)]
mod tests {
    use num_bigint::{BigInt};

    use crate::net::endpoint::Endpoint;
    use crate::net::node::Node;

    #[test]
    fn node_equals_itself() {
        let node = Node::new(
            Endpoint::new("localhost".to_string(), 2330)
        );
        assert!(node.eq(&node))
    }

    #[test]
    fn node_equals_other_node() {
        let node = Node::new(
            Endpoint::new("localhost".to_string(), 2330)
        );
        let other_node = Node::new(
            Endpoint::new("localhost".to_string(), 2330)
        );
        assert!(node.eq(&other_node))
    }

    #[test]
    fn node_does_not_equal_other_node() {
        let node = Node::new(
            Endpoint::new("localhost".to_string(), 2330)
        );
        let other_node = Node::new(
            Endpoint::new("localhost".to_string(), 1982)
        );
        assert!(node.ne(&other_node))
    }

    #[test]
    fn distance_from_other_node() {
        let node = Node::new(
            Endpoint::new("localhost".to_string(), 2330)
        );
        let other_node = Node::new(
            Endpoint::new("localhost".to_string(), 1982)
        );

        let distance = node.distance_from(&other_node);
        let expected_distance_greater_than: u8 = 0;
        assert!(distance.gt(&BigInt::from(expected_distance_greater_than)));
    }

    #[test]
    fn distance_from_itself() {
        let node = Node::new(
            Endpoint::new("localhost".to_string(), 2330)
        );

        let distance = node.distance_from(&node);
        let expected_distance: u8 = 0;
        assert!(distance.eq(&BigInt::from(expected_distance)));
    }
}