use crate::net::endpoint::Endpoint;

const EXPECTED_NODE_ID_LENGTH: usize = 20;

#[derive(Eq, PartialEq)]
pub(crate) struct Node {
    id: Vec<u8>,
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
}

#[cfg(test)]
mod tests {
    use crate::net::endpoint::Endpoint;
    use crate::node::Node;

    #[test]
    #[should_panic]
    fn incorrect_node_id_length() {
        let node = Node::new(
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
            vec![0; 20],
            Endpoint::new("localhost".to_string(), 2330)
        );
        let other_node = Node::new(
            vec![0; 20],
            Endpoint::new("localhost".to_string(), 2330)
        );
        assert!(node.eq(&other_node))
    }

    #[test]
    fn node_does_not_equal_other_node() {
        let node = Node::new(
            vec![0; 20],
            Endpoint::new("localhost".to_string(), 2330)
        );
        let other_node = Node::new(
            vec![1; 20],
            Endpoint::new("localhost".to_string(), 1982)
        );
        assert!(node.ne(&other_node))
    }
}