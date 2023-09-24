use ripemd::{Ripemd160, Digest};

const EXPECTED_ID_LENGTH: usize = 20;

pub(crate) struct Id {
    id: Vec<u8>
}

impl Id {
    pub(crate) fn generate_from(content: String) -> Self {
        let mut hasher = Ripemd160::new();
        hasher.update(content.as_bytes());

        let result = hasher.finalize();
        Id {
            id: result.to_vec()
        }
    }
}

#[cfg(test)]
mod tests {
    use num_bigint::{BigInt, Sign};
    use crate::id::{EXPECTED_ID_LENGTH, Id};
    use hex_literal::hex;

    #[test]
    fn id_as_big_endian() {
        let id = Id::generate_from("Hello world!".to_string());
        assert_eq!(&id.id[..], hex!("7f772647d88750add82d8e1a7a3e5c0902a346a3"));

        let id_as_bigint = BigInt::from_bytes_be(Sign::Plus, &id.id);
        let hex_bigint = format!("{:X}", id_as_bigint);
        assert_eq!(hex_bigint, "7f772647d88750add82d8e1a7a3e5c0902a346a3".to_uppercase());
    }

    #[test]
    fn generate_id() {
        let id = Id::generate_from("localhost:3290".to_string());
        assert_eq!(id.id.len(), EXPECTED_ID_LENGTH);
    }
}