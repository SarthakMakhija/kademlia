use crate::net::endpoint::Endpoint;

#[derive(Eq, PartialEq)]
pub(crate) struct Node {
    id: Vec<u8>,
    endpoint: Endpoint
}

impl Node {
    fn new(id: Vec<u8>, endpoint: Endpoint) -> Self {
        Node {
            id,
            endpoint
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::net::endpoint::Endpoint;
    use crate::node::Node;

    #[test]
    fn node_equals_itself() {
        let node = Node::new(
            vec![10, 11, 12],
            Endpoint::new("localhost".to_string(), 2330)
        );
        assert!(node.eq(&node))
    }

    #[test]
    fn node_equals_other_node() {
        let node = Node::new(
            vec![10, 11, 12],
            Endpoint::new("localhost".to_string(), 2330)
        );
        let other_node = Node::new(
            vec![10, 11, 12],
            Endpoint::new("localhost".to_string(), 2330)
        );
        assert!(node.eq(&other_node))
    }

    #[test]
    fn node_does_not_equal_other_node() {
        let node = Node::new(
            vec![10, 11, 12],
            Endpoint::new("localhost".to_string(), 2330)
        );
        let other_node = Node::new(
            vec![10, 11, 14],
            Endpoint::new("localhost".to_string(), 1982)
        );
        assert!(node.ne(&other_node))
    }
}