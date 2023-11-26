//! Table row interface.
use std::borrow::Cow;

use polars::{
    chunked_array::builder::{BinaryChunkedBuilderCow, Utf8ChunkedBuilderCow},
    prelude::*,
};

pub trait TableRow {
    /// The frame builder type for this row type.
    type Builder: FrameBuilder<Self>;

    /// Get the schema for this table row.
    fn schema() -> Schema;
}

/// Interface for data frame builders.
pub trait FrameBuilder<R>
where
    R: TableRow + ?Sized,
{
    /// Instantiate a frame builder with a specified capacity.
    fn with_capacity(cap: usize) -> Self;
    /// Add a row to the frame builder.
    fn append_row(&mut self, row: &R);
    /// Finish the builder and create a data frame.
    fn build(self) -> PolarsResult<DataFrame>;

    /// Add an iterable of items to the frame.
    fn extend<I, E>(&mut self, iter: I)
    where
        I: IntoIterator<Item = E>,
        E: AsRef<R>,
    {
        for row in iter {
            self.append_row(row.as_ref());
        }
    }
}

/// Trait for column types.
pub trait ColType {
    type PolarsType;
    type Array;
    type Builder;

    /// Create a new builder.
    fn column_builder(name: &str, cap: usize) -> Self::Builder;

    /// Append this item to a builder.
    fn append_to_column(self, b: &mut Self::Builder);
}

macro_rules! col_type {
    ($rs:ty, $pl:ty) => {
        col_type!($rs, $pl, ChunkedArray<$pl>, PrimitiveChunkedBuilder<$pl>);
    };
    ($rs:ty, $pl:ty, $a:ty, $bld: ty) => {
        impl ColType for $rs {
            type PolarsType = $pl;
            type Array = $a;
            type Builder = $bld;

            fn column_builder(name: &str, cap: usize) -> Self::Builder {
                Self::Builder::new(name, cap)
            }

            fn append_to_column(self, b: &mut Self::Builder) {
                b.append_value(self.into());
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
                b.append_option(self.map(Into::into));
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
col_type!(Cow<'_, str>, Utf8Type, Utf8Chunked, Utf8ChunkedBuilderCow);
col_type!(&str, Utf8Type, Utf8Chunked, Utf8ChunkedBuilderCow);
col_type!(String, Utf8Type, Utf8Chunked, Utf8ChunkedBuilderCow);
col_type!(
    Cow<'_, [u8]>,
    BinaryType,
    BinaryChunked,
    BinaryChunkedBuilderCow
);
col_type!(&[u8], BinaryType, BinaryChunked, BinaryChunkedBuilderCow);
