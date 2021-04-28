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

// define primitive types
macro_rules! primitive_arrow_type {
  ($rt:ty, $dt:expr, $array:ty, $builder:ty) => {
    impl ArrowTypeInfo for $rt {
      type PQArray = $array;
      type PQArrayBuilder = $builder;

      fn pq_type() -> DataType {
        $dt
      }
      fn append_to_builder(&self, ab: &mut Self::PQArrayBuilder) -> ArrowResult<()> {
        ab.append_value(*self)
      }
    }
  };
}

primitive_arrow_type!(u8, DataType::UInt8, UInt8Array, UInt8Builder);
primitive_arrow_type!(u16, DataType::UInt16, UInt16Array, UInt16Builder);
primitive_arrow_type!(u32, DataType::UInt32, UInt32Array, UInt32Builder);
primitive_arrow_type!(u64, DataType::UInt64, UInt64Array, UInt64Builder);
primitive_arrow_type!(i8, DataType::Int8, Int8Array, Int8Builder);
primitive_arrow_type!(i16, DataType::Int16, Int16Array, Int16Builder);
primitive_arrow_type!(i32, DataType::Int32, Int32Array, Int32Builder);
primitive_arrow_type!(i64, DataType::Int64, Int64Array, Int64Builder);

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
