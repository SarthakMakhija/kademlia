use std::collections::HashMap;

pub(crate) const EXPECTED_KEY_ID_LENGTH: usize = 20;

pub(crate) type KeyId = Vec<u8>;

pub(crate) struct Key {
    pub(crate) id: KeyId,
    pub(crate) key: Vec<u8>,
}

impl Key {
    pub(crate) fn new(id: Vec<u8>, key: Vec<u8>) -> Self {
        if id.len() != EXPECTED_KEY_ID_LENGTH {
            let error_message = format!("key id length must be {}, but was {}", EXPECTED_KEY_ID_LENGTH, id.len());
            panic!("{}", error_message.as_str())
        }
        Key {
            id,
            key
        }
    }
}

pub(crate) struct StoredValue {
    pub(crate) key_id: KeyId,
    pub(crate) value: Vec<u8>,
}

impl StoredValue {
    pub(crate) fn new(key_id: KeyId, value: Vec<u8>) -> Self {
        StoredValue {
            key_id,
            value
        }
    }

    pub(crate) fn value(&self) -> &Vec<u8> {
        &self.value
    }
}

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
    use crate::store::{EXPECTED_KEY_ID_LENGTH, InMemoryStore, Key, Store};

    #[test]
    #[should_panic]
    fn incorrect_key_id_length() {
        let _key = Key::new(
            vec![10],
            vec![10, 20, 30],
        );
    }

    #[test]
    fn correct_key_id_length() {
        let key = Key::new(
            vec![0; 20],
            vec![10, 20, 30],
        );

        assert_eq!(vec![0; 20], key.id);
        assert_eq!(vec![10, 20, 30], key.key);
    }
    #[test]
    fn get_the_value_for_the_existing_key() {
        let mut store = InMemoryStore::new_in_memory_store();
        let key = "kademlia".as_bytes().to_vec();
        let value = "distributed hash table".as_bytes().to_vec();

        store.put_or_update(Key::new(vec![1; EXPECTED_KEY_ID_LENGTH], key), value);

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

        store.put_or_update(Key::new(vec![1; EXPECTED_KEY_ID_LENGTH], key), value);

        let key_to_delete = "kademlia".as_bytes().to_vec();
        store.delete(&key_to_delete);

        let stored_value = store.get(&key_to_delete);
        assert!(stored_value.is_none(), "{}", format!("value must not be present for {}", String::from_utf8(key_to_delete).unwrap()));
    }
}