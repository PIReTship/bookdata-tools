//! Utility module for serializing with to/from string.
//!
//! This module is designed to be used with `#[serde(with)]`, as in:
//!
//! ```
//! # use serde::{Deserialize, Serialize};
//! # use serde_json::{to_value, json};
//! #[derive(Serialize, Deserialize, Debug)]
//! struct MyStruct {
//!   #[serde(with="bookdata::util::serde_string")]
//!   value: u64
//! }
//!
//! let jsv = to_value(MyStruct { value: 72 }).unwrap();
//! assert_eq!(jsv, json!({
//!   "value": "72"
//! }));
//! ```
//!
//! It will use [ToString] and [FromStr] to serialize and deserialize the data.
use std::fmt;
use std::str::FromStr;
use std::marker::PhantomData;

use serde::{
  Serializer,
  Deserializer,
  de
};

pub fn serialize<S, T: ToString>(v: &T, ser: S) -> Result<S::Ok, S::Error> where S: Serializer {
  let vs = v.to_string();
  ser.serialize_str(vs.as_str())
}

struct FromStrVisitor<T> {
  _ph: PhantomData<T>
}

pub fn deserialize<'de, D, T: FromStr>(de: D) -> Result<T, D::Error> where D: Deserializer<'de> {
  de.deserialize_str(FromStrVisitor { _ph: PhantomData })
}

impl <'de, T: FromStr> de::Visitor<'de> for FromStrVisitor<T> {
  type Value = T;

  fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(formatter, "a parsable string")
  }

  fn visit_str<E: de::Error>(self, s: &str) -> Result<Self::Value, E> {
    s.parse().map_err(|e| {
      de::Error::invalid_value(de::Unexpected::Str(s), &self)
    })
  }
}
