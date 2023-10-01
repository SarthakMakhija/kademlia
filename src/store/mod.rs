use std::cell::RefCell;
use std::collections::HashMap;

use crate::id::Id;

pub(crate) type KeyId = Id;

pub(crate) struct Key {
    pub(crate) id: KeyId,
    pub(crate) key: Vec<u8>,
}

impl Key {
    pub(crate) fn new(key: Vec<u8>) -> Self {
        let key_id = Id::generate_from_bytes(&key);
        Key::new_with_id(key, key_id)
    }

    pub(crate) fn new_with_id(key: Vec<u8>, id: KeyId) -> Self {
        Key { id, key }
    }

    pub(crate) fn length_key_id(&self) -> usize {
        self.id.len()
    }
}

pub(crate) struct StoredValue {
    pub(crate) key_id: KeyId,
    pub(crate) value: Vec<u8>,
}

impl StoredValue {
    pub(crate) fn new(key_id: KeyId, value: Vec<u8>) -> Self {
        StoredValue { key_id, value }
    }

    pub(crate) fn clone_value(&self) -> Vec<u8> {
        self.value.clone()
    }
}

pub(crate) trait Store {
    fn put_or_update(&self, key: Key, value: Vec<u8>);
    fn delete(&self, key: &Vec<u8>);
    fn get(&self, key: &Vec<u8>) -> Option<Vec<u8>>;
}

pub(crate) struct InMemoryStore {
    value_by_key: RefCell<HashMap<Vec<u8>, StoredValue>>,
}

impl InMemoryStore {
    fn new() -> Self {
        InMemoryStore {
            value_by_key: RefCell::new(HashMap::new()),
        }
    }
}

impl Store for InMemoryStore {
    fn put_or_update(&self, key: Key, value: Vec<u8>) {
        self.value_by_key
            .borrow_mut()
            .insert(key.key, StoredValue::new(key.id, value));
    }

    fn delete(&self, key: &Vec<u8>) {
        self.value_by_key.borrow_mut().remove_entry(key);
    }

    fn get(&self, key: &Vec<u8>) -> Option<Vec<u8>> {
        let value_by_key = self.value_by_key.borrow();
        value_by_key
            .get(key)
            .map(|stored_value| stored_value.clone_value())
    }
}

#[cfg(test)]
mod tests {
    use crate::id::EXPECTED_ID_LENGTH_IN_BYTES;
    use crate::store::{InMemoryStore, Key, Store};

    #[test]
    fn key_with_id_and_content() {
        let key = Key::new(vec![10, 20, 30]);

        assert_eq!(EXPECTED_ID_LENGTH_IN_BYTES, key.length_key_id());
        assert_eq!(vec![10, 20, 30], key.key);
    }

    #[test]
    fn get_the_value_for_the_existing_key() {
        let store = InMemoryStore::new();
        let key = "kademlia".as_bytes().to_vec();
        let value = "distributed hash table".as_bytes().to_vec();

        store.put_or_update(Key::new(key), value);

        let query_key = "kademlia".as_bytes().to_vec();
        let stored_value = store.get(&query_key);

        assert!(
            stored_value.is_some(),
            "{}",
            format!(
                "value must be present for {}",
                String::from_utf8(query_key).unwrap()
            )
        );

        let expected_value = "distributed hash table".as_bytes().to_vec();
        assert_eq!(expected_value, stored_value.unwrap())
    }

    #[test]
    fn update_the_value_for_an_existing_key() {
        let store = InMemoryStore::new();
        let key = "kademlia".as_bytes().to_vec();
        let value = "distributed hash table".as_bytes().to_vec();

        store.put_or_update(Key::new(key.clone()), value);

        let updated_value = "hash table".as_bytes().to_vec();
        store.put_or_update(Key::new(key), updated_value);

        let query_key = "kademlia".as_bytes().to_vec();
        let stored_value = store.get(&query_key);

        assert!(
            stored_value.is_some(),
            "{}",
            format!(
                "value must be present for {}",
                String::from_utf8(query_key).unwrap()
            )
        );

        let expected_value = "hash table".as_bytes().to_vec();
        assert_eq!(expected_value, stored_value.unwrap())
    }

    #[test]
    fn get_value_for_the_missing_key() {
        let store = InMemoryStore::new();

        let query_key = "non_existing_key".as_bytes().to_vec();
        let stored_value = store.get(&query_key);

        assert!(
            stored_value.is_none(),
            "{}",
            format!(
                "value must be missing for {}",
                String::from_utf8(query_key).unwrap()
            )
        );
    }

    #[test]
    fn delete_the_value_for_an_existing_key() {
        let store = InMemoryStore::new();
        let key = "kademlia".as_bytes().to_vec();
        let value = "distributed hash table".as_bytes().to_vec();

        store.put_or_update(Key::new(key), value);

        let key_to_delete = "kademlia".as_bytes().to_vec();
        store.delete(&key_to_delete);

        let stored_value = store.get(&key_to_delete);
        assert!(
            stored_value.is_none(),
            "{}",
            format!(
                "value must not be present for {}",
                String::from_utf8(key_to_delete).unwrap()
            )
        );
    }
}
