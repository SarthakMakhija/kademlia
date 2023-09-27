use num_bigint::{BigInt, Sign};
use ripemd::{Digest, Ripemd160};

pub(crate) const EXPECTED_ID_LENGTH_IN_BYTES: usize = 20;

const BITS_IN_BYTE: usize = 8;

#[derive(Eq, PartialEq)]
pub(crate) struct Id {
    pub(crate) id: Vec<u8>,
    id_length_in_bits: usize,
}

impl Id {
    pub(crate) fn generate_from(content: String) -> Self {
        Id::generate_from_bytes(content.as_bytes())
    }

    pub(crate) fn generate_from_bytes(content: &[u8]) -> Self {
        let mut hasher = Ripemd160::new();
        hasher.update(content);

        let result = hasher.finalize();
        Id::new(result.to_vec())
    }

    pub(crate) fn differing_bit_position(&self, other: &Id) -> usize {
        for (index, byte) in self.id.iter().enumerate() {
            let xor = byte ^ other.id[index];
            let position = self.bit_position_set_in(xor);

            if position.1 {
                let byte_index = index * BITS_IN_BYTE;
                let bit_index = position.0;
                return self.id_length_in_bits - (byte_index + bit_index) - 1;
            }
        }
        return 0;
    }

    pub(crate) fn distance_from(&self, other: &Id) -> BigInt {
        let distance: Vec<u8> = self.id.iter()
            .zip(other.id.iter())
            .map(|(&first_id, &other_id)| first_id ^ other_id)
            .collect();

        BigInt::from_bytes_be(Sign::Plus, &distance)
    }

    fn new(id: Vec<u8>) -> Self {
        let id_length_in_bits = id.len() * BITS_IN_BYTE;
        Id {
            id,
            id_length_in_bits
        }
    }

    fn bit_position_set_in(&self, byte: u8) -> (usize, bool) {
        for bit_position in 0..BITS_IN_BYTE {
            if self.is_bit_set(byte, bit_position) {
                return (bit_position, true);
            }
        }
        return (0, false);
    }

    fn is_bit_set(&self, byte: u8, position: usize) -> bool {
        let position = BITS_IN_BYTE - 1 - position;
        let value = byte & (1 << position);
        value > 0
    }
}

#[cfg(test)]
mod tests {
    use num_bigint::{BigInt, Sign};

    use hex_literal::hex;
    use crate::id::{EXPECTED_ID_LENGTH_IN_BYTES, Id};

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
        assert_eq!(id.id.len(), EXPECTED_ID_LENGTH_IN_BYTES);
    }

    #[test]
    fn generate_id_from_bytes() {
        let id = Id::generate_from_bytes("localhost:3290".as_bytes());
        assert_eq!(id.id.len(), EXPECTED_ID_LENGTH_IN_BYTES);
    }

    #[test]
    fn distance_of_id_from_itself() {
        let id = Id::new( vec![0; EXPECTED_ID_LENGTH_IN_BYTES]);
        assert_eq!(BigInt::from(0), id.distance_from(&id));
    }

    #[test]
    fn distance_of_id_from_other_id() {
        let id = Id::new(vec![0; EXPECTED_ID_LENGTH_IN_BYTES]);
        let other_id = Id::new(vec![1; EXPECTED_ID_LENGTH_IN_BYTES]);

        let expected_distance = BigInt::from_bytes_be(Sign::Plus, &vec![1; EXPECTED_ID_LENGTH_IN_BYTES]);
        assert_eq!(expected_distance, id.distance_from(&other_id));
    }

    #[test]
    fn differing_bit_position_among_16_bits_id_1() {
        let id: u16 = 511;       //0000_0001 1111_1111 => big_endian => 1111_1111 1111_0001
        let other_id: u16 = 255; //0000_0000 1111_1111 => big_endian => 1111_1111 0000_0000

        let id = Id::new(id.to_be_bytes().to_vec());
        let other_id = Id::new(other_id.to_be_bytes().to_vec());

        let differing_bit_position = id.differing_bit_position(&other_id);
        assert_eq!(8, differing_bit_position);
    }

    #[test]
    fn differing_bit_position_among_16_bits_id_2() {
        let id: u16 = 247;       //0000_0000 1111_0111 => big_endian => 1111_0111 0000_0000
        let other_id: u16 = 255; //0000_0000 1111_1111 => big_endian => 1111_1111 0000_0000

        let id = Id::new(id.to_be_bytes().to_vec());
        let other_id = Id::new(other_id.to_be_bytes().to_vec());

        let differing_bit_position = id.differing_bit_position(&other_id);
        assert_eq!(3, differing_bit_position);
    }

    #[test]
    fn no_differing_bit_position_among_same_16_bits_id() {
        let id: u16 = 255;       //0000_0000 1111_1111 => big_endian => 1111_1111 0000_0000
        let other_id: u16 = 255; //0000_0000 1111_1111 => big_endian => 1111_1111 0000_0000

        let id = Id::new(id.to_be_bytes().to_vec());
        let other_id = Id::new(other_id.to_be_bytes().to_vec());

        let differing_bit_position = id.differing_bit_position(&other_id);
        assert_eq!(0, differing_bit_position);
    }
}