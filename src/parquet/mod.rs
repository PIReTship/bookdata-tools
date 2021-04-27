pub mod writer;

pub use writer::{TableRow, TableWriter};

use arrow::datatypes::{DataType, Field, ArrowPrimitiveType};
use arrow::array::*;
use arrow::error::{Result as ArrowResult};

pub trait PQAppend<T> {
  fn pq_append_value(&mut self, v: T) -> ArrowResult<()>;
  fn pq_append_option(&mut self, v: Option<T>) -> ArrowResult<()>;
}

impl PQAppend<bool> for BooleanBuilder {
  fn pq_append_value(&mut self, v: bool) -> ArrowResult<()> {
    self.append_value(v)
  }
  fn pq_append_option(&mut self, v: Option<bool>) -> ArrowResult<()> {
    self.append_option(v)
  }
}

impl <A: ArrowPrimitiveType> PQAppend<A::Native> for PrimitiveBuilder<A> {
  fn pq_append_value(&mut self, v: A::Native) -> ArrowResult<()> {
    self.append_value(v)
  }
  fn pq_append_option(&mut self, v: Option<A::Native>) -> ArrowResult<()> {
    self.append_option(v)
  }
}

impl <S: AsRef<str>, O: StringOffsetSizeTrait> PQAppend<S> for GenericStringBuilder<O> {
  fn pq_append_value(&mut self, v: S) -> ArrowResult<()> {
    self.append_value(v)
  }
  fn pq_append_option(&mut self, v: Option<S>) -> ArrowResult<()> {
    match v {
      Some(s) => self.append_value(s),
      None => self.append_null()
    }
  }
}

pub trait ArrowTypeInfo where Self: Sized {
  type PQArray;
  type PQArrayBuilder;

  fn pq_type() -> DataType;
  fn field(name: &str) -> Field {
    Field::new(name, Self::pq_type(), false)
  }
  fn append_to_builder(&self, ab: &mut Self::PQArrayBuilder) -> ArrowResult<()>;
}

impl ArrowTypeInfo for bool {
  type PQArray = BooleanArray;
  type PQArrayBuilder = BooleanBuilder;

  fn pq_type() -> DataType {
    DataType::Boolean
  }
  fn append_to_builder(&self, ab: &mut Self::PQArrayBuilder) -> ArrowResult<()> {
    ab.append_value(*self)
  }
}

impl ArrowTypeInfo for u64 {
  type PQArray = UInt64Array;
  type PQArrayBuilder = UInt64Builder;

  fn pq_type() -> DataType {
    DataType::UInt64
  }
  fn append_to_builder(&self, ab: &mut Self::PQArrayBuilder) -> ArrowResult<()> {
    ab.append_value(*self)
  }
}

impl ArrowTypeInfo for u32 {
  type PQArray = UInt32Array;
  type PQArrayBuilder = UInt32Builder;

  fn pq_type() -> DataType {
    DataType::UInt32
  }
  fn append_to_builder(&self, ab: &mut Self::PQArrayBuilder) -> ArrowResult<()> {
    ab.append_value(*self)
  }
}

impl ArrowTypeInfo for u8 {
  type PQArray = UInt8Array;
  type PQArrayBuilder = UInt8Builder;

  fn pq_type() -> DataType {
    DataType::UInt8
  }
  fn append_to_builder(&self, ab: &mut Self::PQArrayBuilder) -> ArrowResult<()> {
    ab.append_value(*self)
  }
}

impl ArrowTypeInfo for i8 {
  type PQArray = Int8Array;
  type PQArrayBuilder = Int8Builder;

  fn pq_type() -> DataType {
    DataType::Int8
  }
  fn append_to_builder(&self, ab: &mut Self::PQArrayBuilder) -> ArrowResult<()> {
    ab.append_value(*self)
  }
}

impl ArrowTypeInfo for String {
  type PQArray = StringArray;
  type PQArrayBuilder = StringBuilder;

  fn pq_type() -> DataType {
    DataType::Utf8
  }
  fn append_to_builder(&self, ab: &mut Self::PQArrayBuilder) -> ArrowResult<()> {
    ab.append_value(&self)
  }
}

impl <'a> ArrowTypeInfo for &'a str {
  type PQArray = StringArray;
  type PQArrayBuilder = StringBuilder;

  fn pq_type() -> DataType {
    DataType::Utf8
  }
  fn append_to_builder(&self, ab: &mut Self::PQArrayBuilder) -> ArrowResult<()> {
    ab.append_value(self)
  }
}

impl <T> ArrowTypeInfo for Option<T> where T: ArrowTypeInfo + Clone, T::PQArrayBuilder : PQAppend<T> {
  type PQArray = T::PQArray;
  type PQArrayBuilder = T::PQArrayBuilder;

  fn pq_type() -> DataType {
    T::pq_type()
  }
  fn field(name: &str) -> Field {
    Field::new(name, Self::pq_type(), true)
  }
  fn append_to_builder(&self, ab: &mut Self::PQArrayBuilder) -> ArrowResult<()> {
    ab.pq_append_option(self.clone())
  }
}
