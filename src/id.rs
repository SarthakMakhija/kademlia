use num_bigint::{BigInt, Sign};
use ripemd::{Digest, Ripemd160};

pub(crate) const EXPECTED_ID_LENGTH: usize = 20;

#[derive(Eq, PartialEq)]
pub(crate) struct Id {
    pub(crate) id: Vec<u8>,
}

impl Id {
    pub(crate) fn generate_from(content: String) -> Self {
        Id::generate_from_bytes(content.as_bytes())
    }

    pub(crate) fn generate_from_bytes(content: &[u8]) -> Self {
        let mut hasher = Ripemd160::new();
        hasher.update(content);

        let result = hasher.finalize();
        Id {
            id: result.to_vec()
        }
    }

    pub(crate) fn distance_from(&self, other: &Id) -> BigInt {
        let distance: Vec<u8> = self.id.iter()
            .zip(other.id.iter())
            .map(|(&first_id, &other_id)| first_id ^ other_id)
            .collect();

        BigInt::from_bytes_be(Sign::Plus, &distance)
    }
}

#[cfg(test)]
mod tests {
    use num_bigint::{BigInt, Sign};

    use hex_literal::hex;

    use crate::id::{EXPECTED_ID_LENGTH, Id};

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

    #[test]
    fn generate_id_from_bytes() {
        let id = Id::generate_from_bytes("localhost:3290".as_bytes());
        assert_eq!(id.id.len(), EXPECTED_ID_LENGTH);
    }

    #[test]
    fn distance_of_id_from_itself() {
        let id = Id { id: vec![0; EXPECTED_ID_LENGTH] };
        assert_eq!(BigInt::from(0), id.distance_from(&id));
    }

    #[test]
    fn distance_of_id_from_other_id() {
        let id = Id { id: vec![0; EXPECTED_ID_LENGTH] };
        let other_id = Id { id: vec![1; EXPECTED_ID_LENGTH] };

        let expected_distance = BigInt::from_bytes_be(Sign::Plus, &vec![1; EXPECTED_ID_LENGTH]);
        assert_eq!(expected_distance, id.distance_from(&other_id));
    }
}