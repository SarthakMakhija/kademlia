use std::collections::HashSet;

use crate::id::Id;
use crate::net::node::{Node, NodeId};

pub(crate) struct ClosestNeighbors {
    node_ids: HashSet<NodeId>,
    nodes: Vec<Node>,
    target: Id,
    maximum_capacity: usize,
}

impl ClosestNeighbors {
    pub(crate) fn new(maximum_capacity: usize, for_target: Id) -> Self {
        ClosestNeighbors {
            node_ids: HashSet::new(),
            nodes: Vec::with_capacity(maximum_capacity),
            target: for_target,
            maximum_capacity,
        }
    }

    pub(crate) fn add_missing(&mut self, nodes: &Vec<Node>) -> bool {
        for node in nodes {
            if self.nodes.len() < self.maximum_capacity {
                if !self.node_ids.contains(&node.id) {
                    self.node_ids.insert(node.id.clone());
                    self.nodes.push(node.clone());
                }
            } else {
                return false;
            }
        }
        return true;
    }

    pub(crate) fn sort_ascending_by_distance(&mut self) {
        self.nodes
            .sort_by_key(|node| node.id.distance_from(&self.target));
    }
}

#[cfg(test)]
mod tests {
    use crate::id::Id;
    use crate::net::closest_neighbors::ClosestNeighbors;
    use crate::net::endpoint::Endpoint;
    use crate::net::node::Node;

    #[test]
    fn adds_nodes_to_closest_neighbors() {
        let id: u16 = 511;       //0000_0001 1111_1111 => big_endian => 1111_1111 1111_0001
        let target = Id::new(id.to_be_bytes().to_vec());

        let node_a = Node::new_with_id(Endpoint::new("localhost".to_string(), 1239), Id::new(vec![10, 20]));
        let node_b = Node::new_with_id(Endpoint::new("localhost".to_string(), 1243), Id::new(vec![40, 20]));
        let nodes = vec![node_a, node_b];

        let mut closest_neighbors = ClosestNeighbors::new(1, target);
        closest_neighbors.add_missing(&nodes);

        assert_eq!(1, closest_neighbors.nodes.len());
        assert_eq!(1, closest_neighbors.node_ids.len());
    }

    #[test]
    fn adds_unique_nodes_to_closest_neighbors() {
        let id: u16 = 511;       //0000_0001 1111_1111 => big_endian => 1111_1111 1111_0001
        let target = Id::new(id.to_be_bytes().to_vec());

        let node_a = Node::new_with_id(Endpoint::new("localhost".to_string(), 1239), Id::new(vec![10, 20]));
        let node_b = Node::new_with_id(Endpoint::new("localhost".to_string(), 1243), Id::new(vec![40, 20]));
        let node_c = Node::new_with_id(Endpoint::new("localhost".to_string(), 1239), Id::new(vec![10, 20]));
        let nodes = vec![node_a, node_b, node_c];

        let mut closest_neighbors = ClosestNeighbors::new(2, target);
        closest_neighbors.add_missing(&nodes);

        assert_eq!(2, closest_neighbors.nodes.len());
        assert_eq!(2, closest_neighbors.node_ids.len());
    }

    #[test]
    fn sorts_closest_neighbors_by_distance_from_target() {
        let id: u16 = 247;       //0000_0000 1111_0111 => big_endian => 1111_0111 0000_0000
        let target = Id::new(id.to_be_bytes().to_vec());

        let node_a = Node::new_with_id(Endpoint::new("localhost".to_string(), 1243), Id::new(511u16.to_be_bytes().to_vec()));
        let node_b = Node::new_with_id(Endpoint::new("localhost".to_string(), 1239), Id::new(255u16.to_be_bytes().to_vec()));
        let nodes = vec![node_a, node_b];

        let mut closest_neighbors = ClosestNeighbors::new(2, target);
        closest_neighbors.add_missing(&nodes);
        closest_neighbors.sort_ascending_by_distance();

        assert_eq!(&Id::new(255u16.to_be_bytes().to_vec()), &closest_neighbors.nodes[0].id);
        assert_eq!(&Id::new(511u16.to_be_bytes().to_vec()), &closest_neighbors.nodes[1].id);
    }
}