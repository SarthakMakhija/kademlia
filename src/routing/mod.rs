use std::sync::RwLock;

use log::info;

use crate::id::Id;
use crate::net::node::{Node, NodeId};
use crate::routing::neighbors::ClosestNeighbors;

mod neighbors;

const MAX_BUCKET_CAPACITY: usize = 10;

pub(crate) struct Table {
    buckets: Vec<RwLock<Vec<Node>>>,
    node_id: NodeId,
    max_bucket_capacity: usize,
}

impl Table {
    pub(crate) fn new(node_id: NodeId) -> Self {
        Self::new_with_bucket_capacity(node_id, MAX_BUCKET_CAPACITY)
    }

    pub(crate) fn new_with_bucket_capacity(node_id: NodeId, bucket_capacity: usize) -> Self {
        let mut buckets = Vec::with_capacity(node_id.id_length_in_bits);
        (0..node_id.id_length_in_bits).for_each(|_| buckets.push(RwLock::new(Vec::new())));

        Table {
            buckets,
            node_id,
            max_bucket_capacity: bucket_capacity,
        }
    }

    pub(crate) fn add(&self, node: Node) -> (usize, bool) {
        let (bucket_index, contains) = self.contains(&node);
        if !contains {
            let nodes = &mut self.buckets[bucket_index].write().unwrap();
            if nodes.len() < self.max_bucket_capacity {
                info!(
                    "adding node with id {:?} to the bucket with index {}",
                    node.id, bucket_index
                );
                nodes.push(node);
                return (bucket_index, true);
            }
        }
        return (bucket_index, false);
    }

    fn remove(&self, node: &Node) -> bool {
        let (bucket_index, contains) = self.contains(node);
        if contains {
            let mut nodes = self.buckets[bucket_index].write().unwrap();
            let node_index = nodes
                .iter()
                .position(|existing_node| existing_node.eq(node));

            if let Some(index) = node_index {
                info!(
                    "removing node with id {:?} from the bucket with index {}",
                    node.id, bucket_index
                );
                nodes.remove(index);
                return true;
            }
            return false;
        }
        return false;
    }

    pub(crate) fn contains(&self, node: &Node) -> (usize, bool) {
        let bucket_index = self.bucket_index(&node.id);
        let nodes = self.buckets[bucket_index].read().unwrap();

        (bucket_index, nodes.contains(node))
    }

    pub(crate) fn first_node_in(&self, bucket_index: usize) -> Option<Node> {
        assert!(bucket_index < self.node_id.id_length_in_bits);
        let nodes = self.buckets[bucket_index].read().unwrap();
        nodes.get(0).map(|node| node.clone())
    }

    fn closest_neighbors(&self, id: &Id, number_of_neighbors: usize) -> ClosestNeighbors {
        let bucket_index = self.node_id.differing_bit_position(id);
        let mut closest_neighbors = ClosestNeighbors::new(number_of_neighbors, id.clone());

        for bucket_index in self.all_adjacent_bucket_indices(bucket_index) {
            let nodes = self.buckets[bucket_index].read().unwrap();
            if !nodes.is_empty() {
                if !closest_neighbors.add_missing(&nodes) {
                    break;
                }
            }
        }
        info!(
            "returning a total of {} closest neighbors for the id {:?}",
            closest_neighbors.node_ids.len(),
            id
        );
        closest_neighbors.sort_ascending_by_distance();
        return closest_neighbors;
    }

    //TODO: confirm this from the paper
    fn all_adjacent_bucket_indices(&self, bucket_index: usize) -> Vec<usize> {
        let mut low_bucket_index: isize = bucket_index as isize - 1;
        let mut high_bucket_index: usize = bucket_index + 1;

        let mut adjacent_indices = Vec::new();
        adjacent_indices.push(bucket_index);

        while adjacent_indices.len() < self.node_id.id_length_in_bits {
            if high_bucket_index < self.node_id.id_length_in_bits {
                adjacent_indices.push(high_bucket_index);
            }
            if low_bucket_index >= 0 {
                adjacent_indices.push(low_bucket_index as usize);
            }
            high_bucket_index += 1;
            low_bucket_index -= 1;
        }
        return adjacent_indices;
    }

    fn bucket_index(&self, node_id: &NodeId) -> usize {
        let bucket_index = self.node_id.differing_bit_position(node_id);
        assert!(bucket_index < node_id.id_length_in_bits);

        bucket_index
    }
}

#[cfg(test)]
mod tests {
    use crate::id::Id;
    use crate::net::endpoint::Endpoint;
    use crate::net::node::Node;
    use crate::routing::Table;

    #[test]
    fn add_a_node_to_routing_table() {
        let id: u16 = 255;

        let routing_table = Table::new(Id::new(id.to_be_bytes().to_vec()));
        let (_, added) = routing_table.add(Node::new(Endpoint::new("localhost".to_string(), 2379)));
        assert!(added);
    }

    #[test]
    fn do_not_add_an_existing_node_to_routing_table() {
        let id: u16 = 255;

        let routing_table = Table::new(Id::new(id.to_be_bytes().to_vec()));
        let (_, added) = routing_table.add(Node::new(Endpoint::new("localhost".to_string(), 2379)));
        assert!(added);

        let (_, added) = routing_table.add(Node::new(Endpoint::new("localhost".to_string(), 2379)));
        assert_eq!(false, added);
    }

    #[test]
    fn do_not_add_a_node_to_routing_table_if_the_bucket_capacity_is_full() {
        let id: u16 = 255;

        let routing_table = Table::new_with_bucket_capacity(Id::new(id.to_be_bytes().to_vec()), 1);
        let (_, added) = routing_table.add(Node::new_with_id(
            Endpoint::new("localhost".to_string(), 2379),
            Id::new(247u16.to_be_bytes().to_vec()),
        ));
        assert!(added);

        let (_, added) = routing_table.add(Node::new_with_id(
            Endpoint::new("localhost".to_string(), 8989),
            Id::new(247u16.to_be_bytes().to_vec()),
        ));
        assert_eq!(false, added);
    }

    #[test]
    fn remove_an_existing_node() {
        let id: u16 = 255;

        let routing_table = Table::new(Id::new(id.to_be_bytes().to_vec()));
        let (_, added) = routing_table.add(Node::new(Endpoint::new("localhost".to_string(), 2379)));
        assert!(added);

        let node = &Node::new(Endpoint::new("localhost".to_string(), 2379));
        let deleted = routing_table.remove(node);
        assert!(deleted);

        let (_, contains) = routing_table.contains(node);
        assert_eq!(false, contains);
    }

    #[test]
    fn do_not_remove_a_non_existing_node() {
        let id: u16 = 255;
        let routing_table = Table::new(Id::new(id.to_be_bytes().to_vec()));

        let node = &Node::new(Endpoint::new("localhost".to_string(), 1000));
        let deleted = routing_table.remove(node);
        assert_eq!(false, deleted);
    }

    #[test]
    fn contains_an_existing_node() {
        let id: u16 = 511;

        let routing_table = Table::new(Id::new(id.to_be_bytes().to_vec()));
        let (_, added) = routing_table.add(Node::new(Endpoint::new("localhost".to_string(), 2379)));
        assert!(added);

        let node = &Node::new(Endpoint::new("localhost".to_string(), 2379));
        let (_, contains) = routing_table.contains(node);
        assert!(contains);
    }

    #[test]
    fn does_not_contain_a_node() {
        let id: u16 = 511;
        let routing_table = Table::new(Id::new(id.to_be_bytes().to_vec()));

        let node = &Node::new(Endpoint::new("unknown".to_string(), 1010));
        let (_, contains) = routing_table.contains(node);
        assert_eq!(false, contains);
    }

    #[test]
    fn first_node() {
        let id: u16 = 511;

        let routing_table = Table::new(Id::new(id.to_be_bytes().to_vec()));
        let (bucket_index, added) =
            routing_table.add(Node::new(Endpoint::new("localhost".to_string(), 2379)));
        assert!(added);

        let node = routing_table.first_node_in(bucket_index).unwrap();
        assert_eq!("localhost:2379", node.endpoint.address());
    }

    #[test]
    fn first_node_in_an_empty_bucket() {
        let id: u16 = 511;
        let routing_table = Table::new(Id::new(id.to_be_bytes().to_vec()));

        let node = routing_table.first_node_in(0);
        assert!(node.is_none());
    }

    #[test]
    #[should_panic]
    fn first_node_with_invalid_bucket_index() {
        let id: u16 = 511;
        let routing_table = Table::new(Id::new(id.to_be_bytes().to_vec()));

        routing_table.first_node_in(200);
    }

    #[test]
    fn single_closest_neighbor_1() {
        let routing_table = Table::new(Id::new(511u16.to_be_bytes().to_vec()));
        let (_, added) = routing_table.add(Node::new_with_id(
            Endpoint::new("localhost".to_string(), 2379),
            Id::new(511u16.to_be_bytes().to_vec()),
        ));
        assert!(added);

        let (_, added) = routing_table.add(Node::new_with_id(
            Endpoint::new("localhost".to_string(), 2380),
            Id::new(255u16.to_be_bytes().to_vec()),
        ));
        assert!(added);

        let closest_neighbors =
            routing_table.closest_neighbors(&Id::new(255u16.to_be_bytes().to_vec()), 1);
        assert_eq!(
            &Id::new(255u16.to_be_bytes().to_vec()),
            closest_neighbors.node_ids.iter().next().unwrap()
        );
    }

    #[test]
    fn single_closest_neighbor_2() {
        let routing_table = Table::new(Id::new(511u16.to_be_bytes().to_vec()));
        let (_, added) = routing_table.add(Node::new_with_id(
            Endpoint::new("localhost".to_string(), 2379),
            Id::new(511u16.to_be_bytes().to_vec()),
        ));
        assert!(added);

        let (_, added) = routing_table.add(Node::new_with_id(
            Endpoint::new("localhost".to_string(), 2380),
            Id::new(255u16.to_be_bytes().to_vec()),
        ));
        assert!(added);

        let closest_neighbors =
            routing_table.closest_neighbors(&Id::new(510u16.to_be_bytes().to_vec()), 1);
        assert_eq!(
            &Id::new(511u16.to_be_bytes().to_vec()),
            closest_neighbors.node_ids.iter().next().unwrap()
        );
    }

    #[test]
    fn single_closest_neighbor_3() {
        let routing_table = Table::new(Id::new(511u16.to_be_bytes().to_vec()));
        let (_, added) = routing_table.add(Node::new_with_id(
            Endpoint::new("localhost".to_string(), 2379),
            Id::new(511u16.to_be_bytes().to_vec()),
        ));
        assert!(added);

        let (_, added) = routing_table.add(Node::new_with_id(
            Endpoint::new("localhost".to_string(), 2380),
            Id::new(255u16.to_be_bytes().to_vec()),
        ));
        assert!(added);

        let closest_neighbors =
            routing_table.closest_neighbors(&Id::new(247u16.to_be_bytes().to_vec()), 1);
        assert_eq!(
            &Id::new(255u16.to_be_bytes().to_vec()),
            closest_neighbors.node_ids.iter().next().unwrap()
        );
    }

    #[test]
    fn single_closest_neighbor_4() {
        let routing_table = Table::new(Id::new(511u16.to_be_bytes().to_vec()));
        let (_, added) = routing_table.add(Node::new_with_id(
            Endpoint::new("localhost".to_string(), 2379),
            Id::new(511u16.to_be_bytes().to_vec()),
        ));
        assert!(added);

        let (_, added) = routing_table.add(Node::new_with_id(
            Endpoint::new("localhost".to_string(), 2380),
            Id::new(509u16.to_be_bytes().to_vec()),
        ));
        assert!(added);

        let closest_neighbors =
            routing_table.closest_neighbors(&Id::new(255u16.to_be_bytes().to_vec()), 1);
        assert_eq!(
            &Id::new(509u16.to_be_bytes().to_vec()),
            closest_neighbors.node_ids.iter().next().unwrap()
        );
    }
}
