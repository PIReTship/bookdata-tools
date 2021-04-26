pub mod writer;

pub use writer::{TableRow, TableWriter};

use arrow::datatypes::{DataType, Field, ArrowPrimitiveType};
use arrow::array::*;
use arrow::error::{Result as ArrowResult};

pub trait PQAppend<T> {
  fn pq_append_value(&mut self, v: T) -> ArrowResult<()>;
  fn pq_append_option(&mut self, v: Option<T>) -> ArrowResult<()>;
}

impl <A: ArrowPrimitiveType> PQAppend<A::Native> for PrimitiveBuilder<A> {
  fn pq_append_value(&mut self, v: A::Native) -> ArrowResult<()> {
    self.append_value(v)
  }
  fn pq_append_option(&mut self, v: Option<A::Native>) -> ArrowResult<()> {
    self.append_option(v)
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

impl <T> ArrowTypeInfo for Option<T> where T: ArrowTypeInfo + Copy, T::PQArrayBuilder : PQAppend<T> {
  type PQArray = T::PQArray;
  type PQArrayBuilder = T::PQArrayBuilder;

  fn pq_type() -> DataType {
    T::pq_type()
  }
  fn field(name: &str) -> Field {
    Field::new(name, Self::pq_type(), true)
  }
  fn append_to_builder(&self, ab: &mut Self::PQArrayBuilder) -> ArrowResult<()> {
    ab.pq_append_option(*self)
  }
}

/// Define a record for writing to a Parquet table.
#[macro_export]
macro_rules! table_record {
  (struct $rn:ident { $($fn:ident : $ft:ty),* }) => {
    struct $rn {
      $($fn: $ft),*
    }

    paste::paste! {
      struct [<$rn Batch>] {
        $($fn: <$ft as bookdata::parquet::ArrowTypeInfo>::PQArrayBuilder),*
      }
    }

    impl bookdata::parquet::TableRow for $rn {
      paste::paste! {
        type Batch = [<$rn Batch>];
      }

      fn schema() -> arrow::datatypes::Schema {
        arrow::datatypes::Schema::new(vec![
          $(<$ft as bookdata::parquet::ArrowTypeInfo>::field(stringify!($fn))),*
        ])
      }

      fn new_batch(cap: usize) -> Self::Batch {
        Self::Batch {
          $($fn: <$ft as bookdata::parquet::ArrowTypeInfo>::PQArrayBuilder::new(cap)),*
        }
      }

      fn finish_batch(batch: &mut Self::Batch) -> Vec<arrow::array::ArrayRef> {
        vec![
          $(std::sync::Arc::new(batch.$fn.finish())),*
        ]
      }

      fn write_to_batch(&self, batch: &mut Self::Batch) -> anyhow::Result<()> {
        $(
          self.$fn.append_to_builder(&mut batch.$fn)?;
        )*
        Ok(())
      }
    }
  };
}
