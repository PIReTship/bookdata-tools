use arrow::datatypes::{DataType, TimeUnit, Field};
use arrow::array::*;
use arrow::error::{Result as ArrowResult};
use chrono::prelude::*;

// The number of days from 0001-01-01 to 1977-01-01
const EPOCH_DAYS_CE: i32 = 719_163;

pub trait ArrowTypeInfo where Self: Sized {
  type PQArray;
  type PQArrayBuilder;

  fn pq_type() -> DataType;
  fn field(name: &str) -> Field {
    Field::new(name, Self::pq_type(), false)
  }
  fn append_to_builder(&self, ab: &mut Self::PQArrayBuilder) -> ArrowResult<()>;
  fn append_opt_to_builder(opt: Option<Self>, ab: &mut Self::PQArrayBuilder) -> ArrowResult<()>;
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
      fn append_opt_to_builder(opt: Option<Self>, ab: &mut Self::PQArrayBuilder) -> ArrowResult<()> {
        ab.append_option(opt)
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
primitive_arrow_type!(f32, DataType::Float32, Float32Array, Float32Builder);
primitive_arrow_type!(f64, DataType::Float64, Float64Array, Float64Builder);

impl ArrowTypeInfo for String {
  type PQArray = StringArray;
  type PQArrayBuilder = StringBuilder;

  fn pq_type() -> DataType {
    DataType::Utf8
  }
  fn append_to_builder(&self, ab: &mut StringBuilder) -> ArrowResult<()> {
    ab.append_value(&self)
  }
  fn append_opt_to_builder(opt: Option<Self>, ab: &mut StringBuilder) -> ArrowResult<()> {
    if let Some(ref s) = opt {
      ab.append_value(s)
    } else {
      ab.append_null()
    }
  }
}

impl <'a> ArrowTypeInfo for &'a str {
  type PQArray = StringArray;
  type PQArrayBuilder = StringBuilder;

  fn pq_type() -> DataType {
    DataType::Utf8
  }
  fn append_to_builder(&self, ab: &mut StringBuilder) -> ArrowResult<()> {
    ab.append_value(self)
  }
  fn append_opt_to_builder(opt: Option<Self>, ab: &mut StringBuilder) -> ArrowResult<()> {
    if let Some(ref s) = opt {
      ab.append_value(s)
    } else {
      ab.append_null()
    }
  }
}

impl ArrowTypeInfo for NaiveDate {
  type PQArray = Date32Array;
  type PQArrayBuilder = Date32Builder;

  fn pq_type() -> DataType {
    DataType::Date32
  }
  fn append_to_builder(&self, ab: &mut Date32Builder) -> ArrowResult<()> {
    let days = self.num_days_from_ce() - EPOCH_DAYS_CE;
    ab.append_value(days)
  }
  fn append_opt_to_builder(opt: Option<Self>, ab: &mut Date32Builder) -> ArrowResult<()> {
    ab.append_option(opt.map(|d| {
      d.num_days_from_ce() - EPOCH_DAYS_CE
    }))
  }
}

impl ArrowTypeInfo for DateTime<FixedOffset> {
  type PQArray = TimestampSecondArray;
  type PQArrayBuilder = TimestampSecondBuilder;

  fn pq_type() -> DataType {
    DataType::Timestamp(TimeUnit::Second, None)
  }
  fn append_to_builder(&self, ab: &mut TimestampSecondBuilder) -> ArrowResult<()> {
    ab.append_value(self.timestamp())
  }
  fn append_opt_to_builder(opt: Option<Self>, ab: &mut TimestampSecondBuilder) -> ArrowResult<()> {
    ab.append_option(opt.map(|d| {
      d.timestamp()
    }))
  }
}

impl <T> ArrowTypeInfo for Option<T> where T: ArrowTypeInfo + Clone {
  type PQArray = T::PQArray;
  type PQArrayBuilder = T::PQArrayBuilder;

  fn pq_type() -> DataType {
    T::pq_type()
  }
  fn field(name: &str) -> Field {
    Field::new(name, Self::pq_type(), true)
  }
  fn append_to_builder(&self, ab: &mut Self::PQArrayBuilder) -> ArrowResult<()> {
    T::append_opt_to_builder(self.clone(), ab)
  }
  fn append_opt_to_builder(opt: Option<Self>, ab: &mut Self::PQArrayBuilder) -> ArrowResult<()> {
    opt.flatten().append_to_builder(ab)
  }
}
