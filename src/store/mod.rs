mod key;
mod stored_value;

use std::collections::HashMap;
use crate::store::key::Key;
use crate::store::stored_value::StoredValue;

pub(crate) trait Store {
    fn put_or_update(&mut self, key: Key, value: Vec<u8>);
    fn get(&self, key: &Vec<u8>) -> Option<&Vec<u8>>;
    fn delete(&mut self, key: &Vec<u8>);
}

pub(crate) struct InMemoryStore {
    value_by_key: HashMap<Vec<u8>, StoredValue>,
}

impl InMemoryStore {
    fn new_in_memory_store() -> Self {
        InMemoryStore {
            value_by_key: HashMap::new()
        }
    }
}

impl Store for InMemoryStore {
    fn put_or_update(&mut self, key: Key, value: Vec<u8>) {
        self.value_by_key.insert(key.key, StoredValue::new(key.id, value));
    }

    fn get(&self, key: &Vec<u8>) -> Option<&Vec<u8>> {
        self.value_by_key.get(key).map(|stored_value| stored_value.value())
    }

    fn delete(&mut self, key: &Vec<u8>) {
        self.value_by_key.remove_entry(key);
    }
}

#[cfg(test)]
mod tests {
    use crate::store::{InMemoryStore, Store};
    use crate::store::key::Key;

    #[test]
    fn get_the_value_for_the_existing_key() {
        let mut store = InMemoryStore::new_in_memory_store();
        let key = "kademlia".as_bytes().to_vec();
        let value = "distributed hash table".as_bytes().to_vec();

        store.put_or_update(Key::new(vec![1; 20], key), value);

        let query_key = "kademlia".as_bytes().to_vec();
        let stored_value = store.get(&query_key);

        assert!(stored_value.is_some(), "{}", format!("value must be present for {}", String::from_utf8(query_key).unwrap()));

        let expected_value = "distributed hash table".as_bytes().to_vec();
        assert_eq!(&expected_value, stored_value.unwrap())
    }

    #[test]
    fn update_the_value_for_an_existing_key() {
        let mut store = InMemoryStore::new_in_memory_store();
        let key = "kademlia".as_bytes().to_vec();
        let value = "distributed hash table".as_bytes().to_vec();

        store.put_or_update(Key::new(vec![1; 20], key.clone()), value);

        let updated_value = "hash table".as_bytes().to_vec();
        store.put_or_update(Key::new(vec![1; 20], key), updated_value);

        let query_key = "kademlia".as_bytes().to_vec();
        let stored_value = store.get(&query_key);

        assert!(stored_value.is_some(), "{}", format!("value must be present for {}", String::from_utf8(query_key).unwrap()));

        let expected_value = "hash table".as_bytes().to_vec();
        assert_eq!(&expected_value, stored_value.unwrap())
    }

    #[test]
    fn get_value_for_the_missing_key() {
        let store = InMemoryStore::new_in_memory_store();

        let query_key = "non_existing_key".as_bytes().to_vec();
        let stored_value = store.get(&query_key);

        assert!(stored_value.is_none(), "{}", format!("value must be missing for {}", String::from_utf8(query_key).unwrap()));
    }

    #[test]
    fn delete_the_value_for_an_existing_key() {
        let mut store = InMemoryStore::new_in_memory_store();
        let key = "kademlia".as_bytes().to_vec();
        let value = "distributed hash table".as_bytes().to_vec();

        store.put_or_update(Key::new(vec![1; 20], key), value);

        let key_to_delete = "kademlia".as_bytes().to_vec();
        store.delete(&key_to_delete);

        let stored_value = store.get(&key_to_delete);
        assert!(stored_value.is_none(), "{}", format!("value must not be present for {}", String::from_utf8(key_to_delete).unwrap()));
    }
}