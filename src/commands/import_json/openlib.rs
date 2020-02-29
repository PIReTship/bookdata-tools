use std::io::prelude::*;

use super::ops::DataSetOps;
use crate::tsv::split_first;
use crate::cleaning::{write_pgencoded, clean_json};
use crate::error::{Result, err};

/// OpenLibrary data set
pub struct Ops {}

impl DataSetOps for Ops {
  fn schema(&self) -> &'static str {
    "ol"
  }

  fn table_name(&self, tbl: &str) -> String {
    tbl.to_string()   // use the table name as-is
  }

  fn columns(&self, tbl: &str) -> Vec<String> {
    // OpenLibrary column names are just prefixed with the table name
    vec![format!("{}_key", tbl), format!("{}_data", tbl)]
  }

  fn import(&self, src: &mut dyn BufRead, dst: &mut dyn Write) -> Result<usize> {
    let mut jsbuf = String::new();
    let fail = || err("bad line");
    let mut n = 0;
    let mut dbox = Box::new(dst);   // allow dynamic write to be passed to normal methods
    for line in src.lines() {
      let ls = line?;
      let (_ty, rest) = split_first(&ls).ok_or_else(fail)?;
      let (key, rest) = split_first(rest).ok_or_else(fail)?;
      let (_ver, rest) = split_first(rest).ok_or_else(fail)?;
      let (_stamp, json) = split_first(rest).ok_or_else(fail)?;
      clean_json(json, &mut jsbuf);
      dbox.write_all(key.as_bytes())?;
      dbox.write_all(b"\t")?;
      write_pgencoded(&mut dbox, jsbuf.as_bytes())?;
      dbox.write_all(b"\n")?;
      n += 1
    }

    Ok(n)
  }
}
