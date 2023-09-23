const EXPECTED_KEY_ID_LENGTH: usize = 20;

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

#[cfg(test)]
mod tests {
    use crate::store::key::Key;

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


}