//! Table row interface.

use anyhow::Result;
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use polars::{chunked_array::builder::StringChunkedBuilder, prelude::*};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum RowError {
    #[error("required field {0} was null")]
    NullField(&'static str),
    #[error("conversion error: {0}")]
    ConvertError(&'static str),
    #[error("polars error: {0}")]
    Polars(#[from] PolarsError),
}

/// Convert a data frame into a vector.
pub fn iter_df_rows<'a, R>(df: &'a DataFrame) -> Result<FrameRecordIter<'a, R>>
where
    R: TableRow,
{
    let frame = R::Frame::new(df)?;
    Ok(FrameRecordIter {
        frame,
        size: df.height(),
        pos: 0,
    })
}

pub trait TableRow: Sized {
    /// The frame struct for this row type.
    type Frame<'a>: FrameStruct<'a, Self>;
    /// The frame builder type for this row type.
    type Builder: FrameBuilder<Self>;

    /// Get the schema for this table row.
    fn schema() -> Schema;
}

/// Interface for data frame structs for deserialization.
///
/// Frame structs store references to the data frame's columns so we only need
/// to extract them from the frame once.
pub trait FrameStruct<'a, R>
where
    R: TableRow + Sized,
    Self: Sized,
{
    fn new(df: &'a DataFrame) -> PolarsResult<Self>;
    fn read_row(&mut self, idx: usize) -> Result<R, RowError>;
}

/// Interface for data frame builders.
pub trait FrameBuilder<R>
where
    R: TableRow + Sized,
{
    /// Instantiate a frame builder with a specified capacity.
    fn with_capacity(cap: usize) -> Self;
    /// Add a row to the frame builder.
    fn append_row(&mut self, row: R);
    /// Finish the builder and create a data frame.
    fn build(self) -> PolarsResult<DataFrame>;

    /// Add an iterable of items to the frame.
    fn extend<I>(&mut self, iter: I)
    where
        I: IntoIterator<Item = R>,
    {
        for row in iter {
            self.append_row(row);
        }
    }
}

/// Iterator implementation for the rows in a data frame.
pub struct FrameRecordIter<'a, R>
where
    R: TableRow,
{
    frame: R::Frame<'a>,
    size: usize,
    pos: usize,
}

impl<'a, R> Iterator for FrameRecordIter<'a, R>
where
    R: TableRow,
{
    type Item = Result<R, RowError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos < self.size {
            let val = Some(self.frame.read_row(self.pos));
            self.pos += 1;
            val
        } else {
            None
        }
    }
}

/// Trait for column types.
pub trait ColType: Sized {
    type PolarsType;
    type Array;
    type Builder;

    /// Create a new builder.
    fn column_builder(name: &str, cap: usize) -> Self::Builder;

    /// Append this item to a builder.
    fn append_to_column(self, b: &mut Self::Builder);

    /// Cast a series to the appropriate chunked type.
    fn cast_series<'a>(s: &'a Series) -> PolarsResult<&'a Self::Array>;

    /// Read a value from an array.
    fn read_from_column(name: &'static str, a: &Self::Array, pos: usize) -> Result<Self, RowError>;
}

/// Marker trait for column types that can be mapped with Into
pub trait MappableColType: Sized + TryFrom<Self::ColumnType> {
    type ColumnType: ColType + From<Self>;
}

macro_rules! col_type {
    ($rs:ident, $pl:ty) => {
        col_type!($rs, $pl, ChunkedArray<$pl>, PrimitiveChunkedBuilder<$pl>);
    };
    ($rs:ident, $pl:ty, $a:ty, $bld: ty) => {
        col_type!($rs, $pl, $a, $bld, $rs);
    };
    ($rs:ty, $pl:ty, $a:ty, $bld: ty, $cast:ident) => {
        impl ColType for $rs {
            type PolarsType = $pl;
            type Array = $a;
            type Builder = $bld;

            fn column_builder(name: &str, cap: usize) -> Self::Builder {
                Self::Builder::new(name, cap)
            }

            fn append_to_column(self, b: &mut Self::Builder) {
                b.append_value(self);
            }

            fn cast_series<'a>(s: &'a Series) -> PolarsResult<&'a Self::Array> {
                s.$cast()
            }

            fn read_from_column(
                name: &'static str,
                a: &Self::Array,
                pos: usize,
            ) -> Result<Self, RowError> {
                a.get(pos)
                    .ok_or(RowError::NullField(name))
                    .map(|x| x.into())
            }
        }
        // just manually derive the option, bounds are being a pain
        impl ColType for Option<$rs> {
            type PolarsType = $pl;
            type Array = $a;
            type Builder = $bld;

            fn column_builder(name: &str, cap: usize) -> Self::Builder {
                Self::Builder::new(name, cap)
            }

            fn append_to_column(self, b: &mut Self::Builder) {
                b.append_option(self);
            }

            fn cast_series<'a>(s: &'a Series) -> PolarsResult<&'a Self::Array> {
                s.$cast()
            }

            fn read_from_column(
                _name: &'static str,
                a: &Self::Array,
                pos: usize,
            ) -> Result<Self, RowError> {
                Ok(a.get(pos).map(|x| x.into()))
            }
        }
    };
}

col_type!(bool, BooleanType, BooleanChunked, BooleanChunkedBuilder);
col_type!(i8, Int8Type);
col_type!(i16, Int16Type);
col_type!(i32, Int32Type);
col_type!(i64, Int64Type);
col_type!(u8, UInt8Type);
col_type!(u16, UInt16Type);
col_type!(u32, UInt32Type);
col_type!(u64, UInt64Type);
col_type!(f32, Float32Type);
col_type!(f64, Float64Type);
// col_type!(&str, Utf8Type, Utf8Chunked, Utf8ChunkedBuilderCow, utf8);
col_type!(String, StringType, StringChunked, StringChunkedBuilder, str);

// It would be nice to shrink this, but Polars doesn't expose the expected types
// — its date handling only supports operating on chunks, not individual values.
// We use the same logic to convert a date to Parquet's standard “days since the
// epoch” format.
fn convert_naive_date(date: NaiveDate) -> i32 {
    let dt = NaiveDateTime::new(date, NaiveTime::default());
    (dt.timestamp() / (24 * 60 * 60)) as i32
}

fn convert_to_naive_date(ts: i32) -> Result<NaiveDate, RowError> {
    let ts = (ts as i64) * 24 * 60 * 60;
    let dt = NaiveDateTime::from_timestamp_millis(ts * 1000);
    dt.ok_or(RowError::ConvertError("invalid date"))
        .map(|dt| dt.date())
}

impl ColType for NaiveDate {
    type PolarsType = DateType;
    type Array = DateChunked;
    type Builder = PrimitiveChunkedBuilder<Int32Type>;

    fn column_builder(name: &str, cap: usize) -> Self::Builder {
        Self::Builder::new(name, cap)
    }

    fn append_to_column(self, b: &mut Self::Builder) {
        b.append_value(convert_naive_date(self));
    }

    fn cast_series<'a>(s: &'a Series) -> PolarsResult<&'a Self::Array> {
        s.date()
    }

    fn read_from_column(name: &'static str, a: &Self::Array, pos: usize) -> Result<Self, RowError> {
        let res = a.get(pos).map(convert_to_naive_date).transpose()?;
        res.ok_or(RowError::NullField(name))
    }
}

// just manually derive the option, bounds are being a pain
impl ColType for Option<NaiveDate> {
    type PolarsType = DateType;
    type Array = DateChunked;
    type Builder = PrimitiveChunkedBuilder<Int32Type>;

    fn column_builder(name: &str, cap: usize) -> Self::Builder {
        Self::Builder::new(name, cap)
    }

    fn append_to_column(self, b: &mut Self::Builder) {
        b.append_option(self.map(convert_naive_date));
    }

    fn cast_series<'a>(s: &'a Series) -> PolarsResult<&'a Self::Array> {
        s.date()
    }

    fn read_from_column(
        _name: &'static str,
        a: &Self::Array,
        pos: usize,
    ) -> Result<Self, RowError> {
        a.get(pos).map(convert_to_naive_date).transpose()
    }
}

impl<T> ColType for T
where
    T: MappableColType,
{
    type PolarsType = <<T as MappableColType>::ColumnType as ColType>::PolarsType;
    type Array = <<T as MappableColType>::ColumnType as ColType>::Array;
    type Builder = <<T as MappableColType>::ColumnType as ColType>::Builder;

    fn column_builder(name: &str, cap: usize) -> Self::Builder {
        T::ColumnType::column_builder(name, cap)
    }

    fn append_to_column(self, b: &mut Self::Builder) {
        T::ColumnType::from(self).append_to_column(b)
    }

    fn cast_series<'a>(s: &'a Series) -> PolarsResult<&'a Self::Array> {
        T::ColumnType::cast_series(s)
    }

    fn read_from_column(name: &'static str, a: &Self::Array, pos: usize) -> Result<Self, RowError> {
        let val = T::ColumnType::read_from_column(name, a, pos)?;
        val.try_into()
            .map_err(|_| RowError::ConvertError("failed to convert primitive"))
    }
}
