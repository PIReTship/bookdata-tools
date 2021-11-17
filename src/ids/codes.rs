use std::sync::Arc;
use arrow::datatypes::*;
use arrow::array::*;
use datafusion::prelude::create_udf;
use datafusion::physical_plan::functions::make_scalar_function;
use datafusion::physical_plan::udf::ScalarUDF;
use datafusion::execution::context::ExecutionContext;

use log::*;

pub struct NS<'a> {
  pub name: &'a str,
  pub fn_name: &'a str,
  pub code: i32
}

const NS_MULT_BASE: i32 = 100_000_000;

#[allow(dead_code)]
pub const NS_WORK: NS<'static> = NS::new("OL-W", "ol_work", 1);
#[allow(dead_code)]
pub const NS_EDITION: NS<'static> = NS::new("OL-E", "ol_edition", 2);
#[allow(dead_code)]
pub const NS_LOC_REC: NS<'static> = NS::new("LOC", "loc_rec", 3);
#[allow(dead_code)]
pub const NS_GR_WORK: NS<'static> = NS::new("GR-W", "gr_work", 4);
#[allow(dead_code)]
pub const NS_GR_BOOK: NS<'static> = NS::new("GR-B", "gr_book", 5);
#[allow(dead_code)]
pub const NS_LOC_WORK: NS<'static> = NS::new("LOC-W", "loc_work", 6);
#[allow(dead_code)]
pub const NS_LOC_INSTANCE: NS<'static> = NS::new("LOC-I", "loc_instance", 7);
#[allow(dead_code)]
pub const NS_ISBN: NS<'static> = NS::new("ISBN", "isbn", 9);

const NAMESPACES: &'static [&'static NS<'static>] = &[
  &NS_WORK,
  &NS_EDITION,
  &NS_LOC_REC,
  &NS_GR_WORK,
  &NS_GR_BOOK,
  &NS_LOC_WORK,
  &NS_LOC_INSTANCE,
  &NS_ISBN
];

#[cfg(test)]
use quickcheck::quickcheck;

impl <'a> NS<'a> {
  const fn new(name: &'a str, fn_name: &'a str, code: i32) -> NS<'a> {
    NS {
      name, fn_name, code
    }
  }

  #[allow(dead_code)]
  pub fn name(&'a self) -> &'a str {
    self.name
  }

  pub fn code(&'a self) -> i32 {
    self.code
  }

  pub fn base(&'a self) -> i32 {
    self.code() * NS_MULT_BASE
  }

  #[allow(dead_code)]
  pub fn to_code(&'a self, n: i32) -> i32 {
    n + self.base()
  }

  pub fn from_code(&'a self, n: i32) -> Option<i32> {
    let lo = self.base();
    let hi = lo + NS_MULT_BASE;
    if n >= lo && n < hi {
      Some(n - lo)
    } else {
      None
    }
  }
}

impl NS<'static> {
  /// DataFusion UDF to convert from codes.
  pub fn from_code_udf(&'static self) -> ScalarUDF {
    let name = format!("{}_from_code", self.fn_name);
    let func = move |cols: &[ArrayRef]| {
      let arr = cols[0].as_any().downcast_ref::<Int32Array>().unwrap();
      let res: Int32Array = arr.iter().map(|co| match co {
        Some(c) => self.from_code(c),
        None => None
      }).collect();
      Ok(Arc::new(res) as ArrayRef)
    };
    debug!("registering function {}", name);
    create_udf(
      name.as_str(),
      vec![DataType::Int32],
      Arc::new(DataType::Int32),
      make_scalar_function(func))
  }

  /// DataFusion UDF to check codes.
  pub fn code_is_udf(&'static self) -> ScalarUDF {
    let name = format!("code_is_{}", self.fn_name);
    let func = move |cols: &[ArrayRef]| {
      let arr = cols[0].as_any().downcast_ref::<Int32Array>().unwrap();
      let res: BooleanArray = arr.iter().map(|co| match co {
        Some(c) => Some(c / NS_MULT_BASE == self.code()),
        None => None
      }).collect();
      Ok(Arc::new(res) as ArrayRef)
    };
    debug!("registering function {}", name);
    create_udf(
      name.as_str(),
      vec![DataType::Int32],
      Arc::new(DataType::Boolean),
      make_scalar_function(func))
  }
}

/// Add book code UDFs to an execution context.
pub fn add_udfs(ctx: &mut ExecutionContext) {
  for ns in NAMESPACES {
    ctx.register_udf(ns.from_code_udf());
    ctx.register_udf(ns.code_is_udf());
  }
}

/// Get the namespace for a book code.
pub fn ns_of_book_code(code: i32) -> Option<&'static NS<'static>> {
  let pfx = code / NS_MULT_BASE;
  if pfx >= 1 {
    for ns in NAMESPACES {
      if ns.code() == pfx {
        return Some(ns)
      }
    }
  }

  None
}

#[cfg(test)]
quickcheck! {
  fn prop_code_looks_up(code: i32) -> bool {
    if let Some(ns) = ns_of_book_code(code) {
      // mapping worked
      let bc = code % NS_MULT_BASE;
      ns.to_code(bc) == code
    } else {
      // acceptable to not map
      code < NS_MULT_BASE || code > 10*NS_MULT_BASE || code / NS_MULT_BASE == 8
    }
  }
}

#[test]
fn test_to_code() {
  let n = 42;
  let code = NS_LOC_REC.to_code(n);
  assert_eq!(code / NS_MULT_BASE, NS_LOC_REC.code());
}


#[test]
fn test_from_code() {
  let n = 42;
  let code = NS_LOC_REC.to_code(n);
  assert_eq!(NS_LOC_REC.from_code(code), Some(n));
  assert_eq!(NS_EDITION.from_code(code), None);
  assert_eq!(NS_ISBN.from_code(code), None);
}
