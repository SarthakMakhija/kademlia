use crate::id::{EXPECTED_ID_LENGTH_IN_BITS};
use crate::net::node::{Node, NodeId};

pub(crate) struct RoutingTable {
    buckets: Vec<Vec<Node>>,
    node_id: NodeId,
}

impl RoutingTable {
    fn new (node_id: NodeId) -> Self {
        let mut buckets = Vec::with_capacity(EXPECTED_ID_LENGTH_IN_BITS);
        (0..EXPECTED_ID_LENGTH_IN_BITS).for_each(|_| buckets.push(Vec::new()));

        RoutingTable {
            buckets,
            node_id,
        }
    }

    fn add(&mut self, node: Node) -> bool {
        let (bucket_index, contains) = self.contains(&node);
        if !contains {
            self.buckets[bucket_index].push(node);
            return true;
        }
        return false;
    }

    fn remove(&mut self, node: &Node) -> bool {
        let (bucket_index, contains) = self.contains(node);
        if contains {
            let node_index = self.buckets[bucket_index]
                .iter()
                .position(|existing_node| existing_node.eq(node));

            if let Some(index) = node_index {
                self.buckets[bucket_index].remove(index);
                return true;
            }
            return false;
        }
        return false;
    }

    fn contains(&mut self, node: &Node) -> (usize, bool) {
        let bucket_index = self.bucket_index(&node.id);
        let nodes = &self.buckets[bucket_index];

        (bucket_index, nodes.contains(node))
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
    fn remove_an_existing_node() {
        let id: u16 = 255;

        let mut routing_table = RoutingTable::new(Id::new(id.to_be_bytes().to_vec()));
        assert!(routing_table.add(Node::new(Endpoint::new("localhost".to_string(), 2379))));

        let node = &Node::new(Endpoint::new("localhost".to_string(), 2379));
        let deleted = routing_table.remove(node);
        assert!(deleted);

        let (_, contains) = routing_table.contains(node);
        assert_eq!(false, contains);
    }

    #[test]
    fn do_not_remove_a_non_existing_node() {
        let id: u16 = 255;
        let mut routing_table = RoutingTable::new(Id::new(id.to_be_bytes().to_vec()));

        let node = &Node::new(Endpoint::new("localhost".to_string(), 1000));
        let deleted = routing_table.remove(node);
        assert_eq!(false, deleted);
    }

    #[test]
    fn contains_an_existing_node() {
        let id: u16 = 511;

        let mut routing_table = RoutingTable::new(Id::new(id.to_be_bytes().to_vec()));
        assert!(routing_table.add(Node::new(Endpoint::new("localhost".to_string(), 2379))));

        let node = &Node::new(Endpoint::new("localhost".to_string(), 2379));
        let (_, contains) = routing_table.contains(node);
        assert!(contains);
    }

    #[test]
    fn does_not_contain_a_node() {
        let id: u16 = 511;
        let mut routing_table = RoutingTable::new(Id::new(id.to_be_bytes().to_vec()));

        let node = &Node::new(Endpoint::new("unknown".to_string(), 1010));
        let (_, contains) = routing_table.contains(node);
        assert_eq!(false, contains);
    }
}