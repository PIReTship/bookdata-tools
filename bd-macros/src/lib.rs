//! Macros for book data tools.
//!
//! This package implements various procedural macros we use in the book data code.
use proc_macro::{self, TokenStream};
use syn::*;
mod table_row;

/// Macro to derive the TableRow trait for a row.
///
/// Use this as:
///
/// ```
/// #[derive(TableRow)]
/// struct Record {
///     <fields>
/// }
/// ```
#[proc_macro_derive(TableRow)]
pub fn table_row_macro_derive(input: TokenStream) -> TokenStream {
  let ast = parse(input).unwrap();
  table_row::derive_table_row(&ast)
}
