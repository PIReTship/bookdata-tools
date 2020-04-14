use std::io::prelude::*;
use anyhow::Result;

use super::ops::{DataSetOps, process_raw};

/// Amazon data set
pub struct Ops {}

impl DataSetOps for Ops {
  fn schema(&self) -> &'static str {
    "az18"
  }

  fn table_name(&self, tbl: &str) -> String {
    format!("raw_{}", tbl)  // Amazon tables begin with 'raw_'
  }

  fn columns(&self, tbl: &str) -> Vec<String> {
    // Amazon has one import column, containing the data
    vec![format!("{}_data", tbl)]
  }

  fn import(&self, src: &mut dyn BufRead, dst: &mut dyn Write) -> Result<usize> {
    // hack to allow our dynamic read/write objects to work with traditional methods
    let mut sbox = Box::new(src);
    let mut dbox = Box::new(dst);
    // call process_raw
    process_raw(&mut sbox, &mut dbox)
  }
}
