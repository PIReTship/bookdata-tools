use proc_macro::{self, TokenStream};
use quote::{format_ident, quote};
use syn::*;

/// Macro implementation to derive the TableRow trait for a row
pub fn derive_table_row(ast: &syn::DeriveInput) -> TokenStream {
    let name = &ast.ident;
    if !ast.generics.params.is_empty() {
        panic!("generic structs not supported");
    }
    let ff = format_ident!("{}Frame", name);
    let fb = format_ident!("{}FrameBuilder", name);
    let fields = match &ast.data {
        Data::Struct(ds) => match &ds.fields {
            Fields::Named(fs) => fs,
            _ => panic!("table rows must be structs with named fields"),
        },
        _ => panic!("table rows must be structs"),
    };
    // we need vectors of various things related to fields
    // this is how quote!'s iteration works.

    // extract field names
    let f_names: Vec<&Ident> = fields
        .named
        .iter()
        .map(|f| f.ident.as_ref().unwrap())
        .collect();
    let n_fields = f_names.len();
    // field names as strings
    let f_ns: Vec<String> = f_names.iter().map(|i| format!("{}", i)).collect();
    // field types
    let f_types: Vec<&Type> = fields.named.iter().map(|f| &f.ty).collect();

    // field column type
    let f_cts: Vec<_> = f_types
        .iter()
        .map(|t| quote!(#t as crate::arrow::row::ColType))
        .collect();
    // field array types
    let f_atypes: Vec<_> = f_cts.iter().map(|ai| quote!(<#ai>::Array)).collect();
    // field array builder types
    let f_btypes: Vec<_> = f_cts.iter().map(|ai| quote!(<#ai>::Builder)).collect();

    let gen = quote! {
        pub struct #ff<'a> {
            #(#f_names: &'a #f_atypes),*
        }
        pub struct #fb {
            #(#f_names: #f_btypes),*
        }

      impl crate::arrow::TableRow for #name {
        type Frame<'a> = #ff<'a>;
        type Builder = #fb;

        fn schema() -> ::polars::prelude::Schema {
            let mut schema = ::polars::prelude::Schema::with_capacity(#n_fields);
            #(schema.with_column(#f_ns.into(), <<#f_cts>::PolarsType as ::polars::datatypes::PolarsDataType>::get_dtype());)*
            schema
        }
      }

      impl<'a> crate::arrow::row::FrameStruct<'a, #name> for #ff<'a> {
        fn new(df: &'a ::polars::prelude::DataFrame) -> ::polars::prelude::PolarsResult<Self> {
            use crate::arrow::row::ColType;
            Ok(#ff {
                #(#f_names: <#f_types as ColType>::cast_series(df.column(#f_ns)?)?),*
            })
        }
        fn read_row(&mut self, idx: usize) -> ::std::result::Result<#name, crate::arrow::row::RowError> {
            use crate::arrow::row::ColType;
            Ok(#name {
                #(#f_names: <#f_types as ColType>::read_from_column(&self.#f_names, idx)?),*
            })
        }
      }

      impl crate::arrow::row::FrameBuilder<#name> for #fb {
        fn with_capacity(cap: usize) -> Self {
            #fb {
                #(#f_names: <#f_btypes>::new(#f_ns, cap)),*
            }
        }

        fn append_row(&mut self, row: #name) {
            use crate::arrow::row::ColType;
            #(row.#f_names.append_to_column(&mut self.#f_names);)*
        }

        fn build(self) -> ::polars::prelude::PolarsResult<::polars::prelude::DataFrame> {
            use ::polars::prelude::{ChunkedBuilder, IntoSeries, DataFrame};
            let cols = vec![
                #(self.#f_names.finish().into_series()),*
            ];
            DataFrame::new(cols)
        }
      }
    };
    gen.into()
}
