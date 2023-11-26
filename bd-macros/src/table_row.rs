use proc_macro::{self, TokenStream};
use quote::{format_ident, quote};
use syn::*;

/// Macro implementation to derive the TableRow trait for a row
pub fn derive_table_row(ast: &syn::DeriveInput) -> TokenStream {
    let name = &ast.ident;
    if !ast.generics.params.is_empty() {
        panic!("generic structs not supported");
    }
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
    // field array builer types
    let f_btypes: Vec<_> = f_cts.iter().map(|ai| quote!(<#ai>::Builder)).collect();

    let gen = quote! {
      pub struct #fb {
        #(#f_names: #f_btypes),*
      }

      impl crate::arrow::TableRow for #name {
        type Builder = #fb;

        fn schema() -> ::polars::prelude::Schema {
            let schema = ::polars::prelude::Schema::with_capacity(#n_fields);
            #(schema.with_column(#f_ns.into(), <#f_cts>::PolarsType::get_dtype());)*
        }
      }

      impl crate::arrow::FrameBuilder<#name> for #fb {
        fn with_capacity(cap: usize) -> Self {
            #fb {
                #(#f_names: <#f_btypes>::new(#f_ns, cap)),*
            }
        }

        fn append_row(&mut self, row: &#name) {
            #(row.append_to_column(&mut self.#f_names);)*
        }

        fn build(self) -> ::polars::prelude::PolarsResult<::polars::prelude::DataFrame> {
            let cols = vec![
                #(self.#f_names.build().into_series()),*
            ];
            Ok(::polars::prelude::DataFrame::new(cols))
        }
      }
    };
    gen.into()
}
