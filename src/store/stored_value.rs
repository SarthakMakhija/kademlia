use crate::store::key::KeyId;

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