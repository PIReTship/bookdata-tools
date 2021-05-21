use arrow::datatypes::{DataType, TimeUnit, Field};
use arrow::array::*;
use arrow::error::{Result as ArrowResult};
use chrono::prelude::*;
// use uuid::Uuid;
use paste::paste;

// The number of days from 0001-01-01 to 1977-01-01
const EPOCH_DAYS_CE: i32 = 719_163;

/// Trait defining how to store a type in Arrow.
pub trait ArrowTypeInfo where Self: Sized {
  type Array;
  type ArrayBuilder;

  fn new_builder(cap: usize) -> Self::ArrayBuilder;

  fn pq_type() -> DataType;
  fn field(name: &str) -> Field {
    Field::new(name, Self::pq_type(), false)
  }
  fn append_to_builder(&self, ab: &mut Self::ArrayBuilder) -> ArrowResult<()>;
  fn append_opt_to_builder(opt: Option<Self>, ab: &mut Self::ArrayBuilder) -> ArrowResult<()>;
}

/// Trait to declare that a type should be serialized as a different Arrow type.
pub trait ArrowTypeWrapper where Self: Sized + Clone {
  type Wrapped: Sized + From<Self> + ArrowTypeInfo;
}

impl <T: ArrowTypeWrapper + Sized> ArrowTypeInfo for T {
  type Array = <T::Wrapped as ArrowTypeInfo>::Array;
  type ArrayBuilder = <T::Wrapped as ArrowTypeInfo>::ArrayBuilder;

  fn new_builder(cap: usize) -> Self::ArrayBuilder {
    <T::Wrapped as ArrowTypeInfo>::new_builder(cap)
  }

  fn pq_type() -> DataType {
    <T::Wrapped as ArrowTypeInfo>::pq_type()
  }
  fn append_to_builder(&self, ab: &mut Self::ArrayBuilder) -> ArrowResult<()> {
    let clone = self.clone();
    let w: T::Wrapped = clone.into();
    w.append_to_builder(ab)
  }
  fn append_opt_to_builder(opt: Option<Self>, ab: &mut Self::ArrayBuilder) -> ArrowResult<()> {
    <T::Wrapped as ArrowTypeInfo>::append_opt_to_builder(opt.map(|v| v.into()), ab)
  }
}

// define type info for a primitive type
macro_rules! primitive_arrow_type {
  ($rt:ty, $atype:ident, $pqa:ident) => {
    paste! {
      impl ArrowTypeInfo for $rt {
        type Array = [<$atype Array>];
        type ArrayBuilder = [<$atype Builder>];

        fn new_builder(cap: usize) -> Self::ArrayBuilder {
          Self::ArrayBuilder::new(cap)
        }

        fn pq_type() -> DataType {
          DataType::$atype
        }
        fn append_to_builder(&self, ab: &mut Self::ArrayBuilder) -> ArrowResult<()> {
          ab.append_value(*self)
        }
        fn append_opt_to_builder(opt: Option<Self>, ab: &mut Self::ArrayBuilder) -> ArrowResult<()> {
          ab.append_option(opt)
        }
      }
    }
  };
}

primitive_arrow_type!(u8, UInt8, ubyte);
primitive_arrow_type!(u16, UInt16, ushort);
primitive_arrow_type!(u32, UInt32, uint);
primitive_arrow_type!(u64, UInt64, ulong);
primitive_arrow_type!(i8, Int8, byte);
primitive_arrow_type!(i16, Int16, short);
primitive_arrow_type!(i32, Int32, int);
primitive_arrow_type!(i64, Int64, long);
primitive_arrow_type!(f32, Float32, float);
primitive_arrow_type!(f64, Float64, double);

impl ArrowTypeInfo for String {
  type Array = StringArray;
  type ArrayBuilder = StringBuilder;

  fn new_builder(cap: usize) -> Self::ArrayBuilder {
    StringBuilder::new(cap)
  }

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

impl ArrowTypeInfo for NaiveDate {
  type Array = Date32Array;
  type ArrayBuilder = Date32Builder;

  fn new_builder(cap: usize) -> Self::ArrayBuilder {
    Self::ArrayBuilder::new(cap)
  }

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

impl ArrowTypeInfo for NaiveDateTime {
  type Array = TimestampMillisecondArray;
  type ArrayBuilder = TimestampMillisecondBuilder;

  fn new_builder(cap: usize) -> Self::ArrayBuilder {
    Self::ArrayBuilder::new(cap)
  }

  fn pq_type() -> DataType {
    DataType::Timestamp(TimeUnit::Millisecond, None)
  }
  fn append_to_builder(&self, ab: &mut Self::ArrayBuilder) -> ArrowResult<()> {
    ab.append_value(self.timestamp_millis())
  }
  fn append_opt_to_builder(opt: Option<Self>, ab: &mut Self::ArrayBuilder) -> ArrowResult<()> {
    ab.append_option(opt.map(|d| {
      d.timestamp_millis()
    }))
  }
}

impl ArrowTypeInfo for DateTime<FixedOffset> {
  type Array = TimestampSecondArray;
  type ArrayBuilder = TimestampSecondBuilder;

  fn new_builder(cap: usize) -> Self::ArrayBuilder {
    Self::ArrayBuilder::new(cap)
  }

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

// impl ArrowTypeInfo for Uuid {
//   type Array = FixedSizeBinaryArray;
//   type ArrayBuilder = FixedSizeBinaryBuilder;

//   fn new_builder(cap: usize) -> Self::ArrayBuilder {
//     FixedSizeBinaryBuilder::new(cap * 8, 8)
//   }

//   fn pq_type() -> DataType {
//     DataType::FixedSizeBinary(8)
//   }
//   fn append_to_builder(&self, ab: &mut Self::ArrayBuilder) -> ArrowResult<()> {
//     let bytes = self.as_bytes();
//     ab.append_value(bytes)
//   }
//   fn append_opt_to_builder(opt: Option<Self>, ab: &mut Self::ArrayBuilder) -> ArrowResult<()> {
//     if let Some(id) = opt {
//       id.append_to_builder(ab)
//     } else {
//       ab.append_null()
//     }
//   }
// }

impl <T> ArrowTypeInfo for Option<T> where T: ArrowTypeInfo + Clone {
  type Array = T::Array;
  type ArrayBuilder = T::ArrayBuilder;

  fn new_builder(cap: usize) -> Self::ArrayBuilder {
    T::new_builder(cap)
  }

  fn pq_type() -> DataType {
    T::pq_type()
  }
  fn field(name: &str) -> Field {
    Field::new(name, Self::pq_type(), true)
  }
  fn append_to_builder(&self, ab: &mut Self::ArrayBuilder) -> ArrowResult<()> {
    T::append_opt_to_builder(self.clone(), ab)
  }
  fn append_opt_to_builder(opt: Option<Self>, ab: &mut Self::ArrayBuilder) -> ArrowResult<()> {
    opt.flatten().append_to_builder(ab)
  }
}
