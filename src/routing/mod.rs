use crate::id::Id;
use crate::net::node::{Node, NodeId};
use crate::routing::neighbors::ClosestNeighbors;
use log::info;
use std::cell::RefCell;

mod neighbors;

pub(crate) struct Table {
    buckets: RefCell<Vec<Vec<Node>>>,
    node_id: NodeId,
}

impl Table {
    pub(crate) fn new(node_id: NodeId) -> Self {
        let mut buckets = Vec::with_capacity(node_id.id_length_in_bits);
        (0..node_id.id_length_in_bits).for_each(|_| buckets.push(Vec::new()));

        Table {
            buckets: RefCell::new(buckets),
            node_id,
        }
    }

    pub(crate) fn add(&self, node: Node) -> bool {
        let (bucket_index, contains) = self.contains(&node);
        if !contains {
            info!(
                "adding node with id {:?} to the bucket with index {}",
                node.id, bucket_index
            );
            self.buckets.borrow_mut()[bucket_index].push(node);
            return true;
        }
        return false;
    }

    fn remove(&self, node: &Node) -> bool {
        let (bucket_index, contains) = self.contains(node);
        if contains {
            let node_index = self.buckets.borrow_mut()[bucket_index]
                .iter()
                .position(|existing_node| existing_node.eq(node));

            if let Some(index) = node_index {
                info!(
                    "removing node with id {:?} from the bucket with index {}",
                    node.id, bucket_index
                );
                self.buckets.borrow_mut()[bucket_index].remove(index);
                return true;
            }
            return false;
        }
        return false;
    }

    fn contains(&self, node: &Node) -> (usize, bool) {
        let bucket_index = self.bucket_index(&node.id);
        let nodes = &self.buckets.borrow()[bucket_index];

        (bucket_index, nodes.contains(node))
    }

    fn closest_neighbors(&self, id: &Id, number_of_neighbors: usize) -> ClosestNeighbors {
        let bucket_index = self.node_id.differing_bit_position(id);
        let mut closest_neighbors = ClosestNeighbors::new(number_of_neighbors, id.clone());

        for bucket_index in self.all_adjacent_bucket_indices(bucket_index) {
            if !self.buckets.borrow()[bucket_index].is_empty() {
                let nodes = &self.buckets.borrow()[bucket_index];
                if !closest_neighbors.add_missing(nodes) {
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
        assert!(routing_table.add(Node::new(Endpoint::new("localhost".to_string(), 2379))))
    }

    #[test]
    fn do_not_add_an_existing_node_to_routing_table() {
        let id: u16 = 255;

        let routing_table = Table::new(Id::new(id.to_be_bytes().to_vec()));
        assert!(routing_table.add(Node::new(Endpoint::new("localhost".to_string(), 2379))));
        assert_eq!(
            false,
            routing_table.add(Node::new(Endpoint::new("localhost".to_string(), 2379)))
        )
    }

    #[test]
    fn remove_an_existing_node() {
        let id: u16 = 255;

        let routing_table = Table::new(Id::new(id.to_be_bytes().to_vec()));
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
        let routing_table = Table::new(Id::new(id.to_be_bytes().to_vec()));

        let node = &Node::new(Endpoint::new("localhost".to_string(), 1000));
        let deleted = routing_table.remove(node);
        assert_eq!(false, deleted);
    }

    #[test]
    fn contains_an_existing_node() {
        let id: u16 = 511;

        let routing_table = Table::new(Id::new(id.to_be_bytes().to_vec()));
        assert!(routing_table.add(Node::new(Endpoint::new("localhost".to_string(), 2379))));

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
    fn single_closest_neighbor_1() {
        let routing_table = Table::new(Id::new(511u16.to_be_bytes().to_vec()));
        assert!(routing_table.add(Node::new_with_id(
            Endpoint::new("localhost".to_string(), 2379),
            Id::new(511u16.to_be_bytes().to_vec()),
        )));
        assert!(routing_table.add(Node::new_with_id(
            Endpoint::new("localhost".to_string(), 2380),
            Id::new(255u16.to_be_bytes().to_vec()),
        )));

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
        assert!(routing_table.add(Node::new_with_id(
            Endpoint::new("localhost".to_string(), 2379),
            Id::new(511u16.to_be_bytes().to_vec()),
        )));
        assert!(routing_table.add(Node::new_with_id(
            Endpoint::new("localhost".to_string(), 2380),
            Id::new(255u16.to_be_bytes().to_vec()),
        )));

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
        assert!(routing_table.add(Node::new_with_id(
            Endpoint::new("localhost".to_string(), 2379),
            Id::new(511u16.to_be_bytes().to_vec()),
        )));
        assert!(routing_table.add(Node::new_with_id(
            Endpoint::new("localhost".to_string(), 2380),
            Id::new(255u16.to_be_bytes().to_vec()),
        )));

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
        assert!(routing_table.add(Node::new_with_id(
            Endpoint::new("localhost".to_string(), 2379),
            Id::new(511u16.to_be_bytes().to_vec()),
        )));
        assert!(routing_table.add(Node::new_with_id(
            Endpoint::new("localhost".to_string(), 2380),
            Id::new(509u16.to_be_bytes().to_vec()),
        )));

        let closest_neighbors =
            routing_table.closest_neighbors(&Id::new(255u16.to_be_bytes().to_vec()), 1);
        assert_eq!(
            &Id::new(509u16.to_be_bytes().to_vec()),
            closest_neighbors.node_ids.iter().next().unwrap()
        );
    }
}
