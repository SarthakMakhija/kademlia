use crate::id::{EXPECTED_ID_LENGTH_IN_BITS};
use crate::net::node::{Node, NodeId};

pub(crate) struct RoutingTable {
    buckets: Vec<Vec<Node>>,
    node_id: NodeId,
}

impl RoutingTable {
    pub(crate) fn new (node_id: NodeId) -> Self {
        let mut buckets = Vec::with_capacity(EXPECTED_ID_LENGTH_IN_BITS);
        (0..EXPECTED_ID_LENGTH_IN_BITS).for_each(|_| buckets.push(Vec::new()));

        RoutingTable {
            buckets,
            node_id,
        }
    }

    fn add(&mut self, node: Node) -> bool {
        let bucket_index = self.bucket_index(&node.id);
        let nodes = &self.buckets[bucket_index];

        if !nodes.contains(&node) {
            self.buckets[bucket_index].push(node);
            return true;
        }
        return false;
    }

    fn contains(&mut self, node: &Node) -> bool {
        let bucket_index = self.bucket_index(&node.id);
        let nodes = &self.buckets[bucket_index];
        
        nodes.contains(node)
    }

    fn bucket_index(&mut self, node_id: &NodeId) -> usize {
        let bucket_index = self.node_id.differing_bit_position(node_id);
        assert!(bucket_index > 0);
        assert!(bucket_index < EXPECTED_ID_LENGTH_IN_BITS);

        bucket_index
    }
}

#[cfg(test)]
mod tests {
    use crate::id::Id;
    use crate::net::endpoint::Endpoint;
    use crate::net::node::Node;
    use crate::net::routing_table::RoutingTable;

    #[test]
    fn add_a_node_to_routing_table() {
        let id: u16 = 255;

        let mut routing_table = RoutingTable::new(Id::new(id.to_be_bytes().to_vec()));
        assert!(routing_table.add(Node::new(Endpoint::new("localhost".to_string(), 2379))))
    }

    #[test]
    fn do_not_add_an_existing_node_to_routing_table() {
        let id: u16 = 255;

        let mut routing_table = RoutingTable::new(Id::new(id.to_be_bytes().to_vec()));
        assert!(routing_table.add(Node::new(Endpoint::new("localhost".to_string(), 2379))));
        assert_eq!(false, routing_table.add(Node::new(Endpoint::new("localhost".to_string(), 2379))))
    }

    #[test]
    fn contains_a_node() {
        let id: u16 = 511;

        let mut routing_table = RoutingTable::new(Id::new(id.to_be_bytes().to_vec()));
        assert!(routing_table.add(Node::new(Endpoint::new("localhost".to_string(), 2379))));

        let node = &Node::new(Endpoint::new("localhost".to_string(), 2379));
        assert!(routing_table.contains(node));
    }
}