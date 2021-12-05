//! Decoders for binary data types.
use std::array::TryFromSliceError;

use thiserror::Error;
use hex;

/// Error type for hex decoding operations.
#[derive(Error, Debug)]
pub enum HexDecodeError {
  #[error("could not decode hex data")]
  Decode(#[from] hex::FromHexError),
  #[error("decoded data incorrect size")]
  Split(#[from] TryFromSliceError),
}

/// Decode a 128-bit hex string into a pair of i64s.
pub fn decode_hex_i64_pair(data: &str) -> Result<(i64, i64), HexDecodeError> {
  let data = hex::decode(data)?;
  let (hi, lo) = data.split_at(8);

  let hi: [u8; 8] = hi.try_into()?;
  let lo: [u8; 8] = lo.try_into()?;

  let hi = i64::from_be_bytes(hi);
  let lo = i64::from_be_bytes(lo);

  Ok((hi, lo))
}

#[test]
fn test_decode_zero_pair() {
  let zeros = "00000000000000000000000000000000";
  let (hi, lo) = decode_hex_i64_pair(zeros).unwrap();
  assert_eq!(hi, 0);
  assert_eq!(lo, 0);
}

#[test]
fn test_decode_n_pair() {
  let zeros = "80000000000000000000000000000001";
  let (hi, lo) = decode_hex_i64_pair(zeros).unwrap();
  assert_eq!(hi, i64::MIN);
  assert_eq!(lo, 1);
}
