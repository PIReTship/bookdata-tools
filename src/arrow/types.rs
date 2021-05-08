use arrow::datatypes::{DataType, TimeUnit, Field};
use arrow::array::*;
use arrow::error::{Result as ArrowResult};
use parquet::record::RowAccessor;
use parquet::errors::{ParquetError, Result as PQResult};
use chrono::prelude::*;
use paste::paste;

// The number of days from 0001-01-01 to 1977-01-01
const EPOCH_DAYS_CE: i32 = 719_163;

pub trait ArrowTypeInfo where Self: Sized {
  type Array;
  type ArrayBuilder;

  fn pq_type() -> DataType;
  fn field(name: &str) -> Field {
    Field::new(name, Self::pq_type(), false)
  }
  fn append_to_builder(&self, ab: &mut Self::ArrayBuilder) -> ArrowResult<()>;
  fn append_opt_to_builder(opt: Option<Self>, ab: &mut Self::ArrayBuilder) -> ArrowResult<()>;

  fn read_from_pq_row<R: RowAccessor>(row: &R, _i: usize) -> PQResult<Self> {
    Err(ParquetError::General("read_from_pq_row not implemented for this type".to_owned()))
  }
}

// define type info for a primitive type
macro_rules! primitive_arrow_type {
  ($rt:ty, $atype:ident, $pqa:ident) => {
    paste! {
      impl ArrowTypeInfo for $rt {
        type Array = [<$atype Array>];
        type ArrayBuilder = [<$atype Builder>];

        fn pq_type() -> DataType {
          DataType::$atype
        }
        fn append_to_builder(&self, ab: &mut Self::ArrayBuilder) -> ArrowResult<()> {
          ab.append_value(*self)
        }
        fn append_opt_to_builder(opt: Option<Self>, ab: &mut Self::ArrayBuilder) -> ArrowResult<()> {
          ab.append_option(opt)
        }

        fn read_from_pq_row<R: RowAccessor>(row: &R, i: usize) -> PQResult<Self> {
          row.[<get_ $pqa>](i)
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

  fn read_from_pq_row<R: RowAccessor>(row: &R, i: usize) -> PQResult<Self> {
    row.get_string(i).map(|s| s.to_owned())
  }
}

impl ArrowTypeInfo for NaiveDate {
  type Array = Date32Array;
  type ArrayBuilder = Date32Builder;

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

  fn read_from_pq_row<R: RowAccessor>(row: &R, i: usize) -> PQResult<Self> {
    let dt = row.get_timestamp_millis(i).map(|ts| {
      NaiveDateTime::from_timestamp((ts / 1000) as i64, (ts % 1000) as u32)
    }).or_else(|_e| {
      row.get_timestamp_micros(i).map(|ts| {
        NaiveDateTime::from_timestamp((ts / 1_000_000) as i64, (ts % 1_000_000) as u32)
      })
    })?;
    Ok(dt.date())
  }
}

impl ArrowTypeInfo for NaiveDateTime {
  type Array = TimestampMillisecondArray;
  type ArrayBuilder = TimestampMillisecondBuilder;

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

  fn read_from_pq_row<R: RowAccessor>(row: &R, i: usize) -> PQResult<Self> {
    row.get_timestamp_millis(i).map(|ts| {
      NaiveDateTime::from_timestamp(ts as i64, 0)
    }).or_else(|_e| {
      row.get_timestamp_micros(i).map(|ts| {
        NaiveDateTime::from_timestamp((ts / 1000) as i64, (ts % 1000) as u32)
      })
    })
  }
}

impl ArrowTypeInfo for DateTime<FixedOffset> {
  type Array = TimestampSecondArray;
  type ArrayBuilder = TimestampSecondBuilder;

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
  type Array = T::Array;
  type ArrayBuilder = T::ArrayBuilder;

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
