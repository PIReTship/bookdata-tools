use proc_macro::{self, TokenStream};
use quote::{quote, format_ident};
use syn::*;

/// Macro implementation to derive the TableRow trait for a row
pub fn derive_table_row(ast: &syn::DeriveInput) -> TokenStream {
  let name = &ast.ident;
  if !ast.generics.params.is_empty() {
    panic!("generic structs not supported");
  }
  let batch = format_ident!("{}Batch", name);
  let fields = match &ast.data {
    Data::Struct(ds) => match &ds.fields {
      Fields::Named(fs) => fs,
      _ => panic!("table rows must be structs with named fields")
    },
    _ => panic!("table rows must be structs")
  };
  // we need vectors of variou things related to fields
  // this is how quote!'s iteration works.

  // extract field names
  let f_names: Vec<&Ident> = fields.named.iter().map(|f| {
    f.ident.as_ref().unwrap()
  }).collect();
  // field names as strings
  let f_ns: Vec<String> = f_names.iter().map(|i| {
    format!("{}", i)
  }).collect();
  // field types
  let f_types: Vec<&Type> = fields.named.iter().map(|f| {
    &f.ty
  }).collect();

  // field ArrowTypeInfo
  let f_ainfo: Vec<_> = f_types.iter().map(|t| {
    quote!(#t as bookdata::arrow::ArrowTypeInfo)
  }).collect();
  // field array builer types
  let f_btypes: Vec<_> = f_ainfo.iter().map(|ai| {
    quote!(<#ai>::ArrayBuilder)
  }).collect();

  // extract field attributes
  let f_props: Vec<_> = fields.named.iter().flat_map(|f| {
    let name = format!("{}", f.ident.as_ref().unwrap());
    let attr = f.attrs.iter().find(|a| {
      a.path.is_ident("parquet")
    });
    if let Some(attr) = attr {
      let meta = attr.parse_meta().expect("valid meta-attribute");
      if let Meta::List(ml) = meta {
        let mut props = Vec::new();
        for kid in ml.nested {
          match kid {
            NestedMeta::Meta(Meta::Path(p)) if p.is_ident("statistics") => {
              props.push(quote! {
                let props = props.set_column_statistics_enabled(#name.into(), true);
              });
            },
            bad => panic!("invalid attribute: {:?}", bad)
          }
        }
        return props
      } else {
        panic!("invalid meta-attribute syntax")
      }
    } else {
      Vec::new()
    }
  }).collect();

  let gen = quote! {
    pub struct #batch {
      #(#f_names: #f_btypes),*
    }

    impl bookdata::arrow::TableRow for #name {
      type Batch = #batch;

      fn schema() -> arrow::datatypes::Schema {
        arrow::datatypes::Schema::new(vec![
          #(<#f_ainfo>::field(#f_ns)),*
        ])
      }

      fn new_batch(cap: usize) -> Self::Batch {
        Self::Batch {
          #(#f_names: <#f_ainfo>::new_builder(cap)),*
        }
      }

      fn adjust_writer_props(props: parquet::file::properties::WriterPropertiesBuilder) -> parquet::file::properties::WriterPropertiesBuilder {
        #(#f_props)*
        props
      }

      fn finish_batch(batch: &mut Self::Batch) -> Vec<arrow::array::ArrayRef> {
        vec![
          #(std::sync::Arc::new(batch.#f_names.finish())),*
        ]
      }

      fn write_to_batch(&self, batch: &mut Self::Batch) -> anyhow::Result<()> {
        #(
          self.#f_names.append_to_builder(&mut batch.#f_names)?;
        )*
        Ok(())
      }
    }
  };
  gen.into()
}