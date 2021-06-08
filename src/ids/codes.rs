pub struct NS<'a>(&'a str, i32);

const NS_MULT_BASE: i32 = 100_000_000;

#[allow(dead_code)]
pub const NS_WORK: NS<'static> = NS("OL-W", 1);
#[allow(dead_code)]
pub const NS_EDITION: NS<'static> = NS("OL-E", 2);
#[allow(dead_code)]
pub const NS_LOC_REC: NS<'static> = NS("LOC", 3);
#[allow(dead_code)]
pub const NS_GR_WORK: NS<'static> = NS("GR-W", 4);
#[allow(dead_code)]
pub const NS_GR_BOOK: NS<'static> = NS("GR-B", 5);
#[allow(dead_code)]
pub const NS_LOC_WORK: NS<'static> = NS("LOC-W", 6);
#[allow(dead_code)]
pub const NS_LOC_INSTANCE: NS<'static> = NS("LOC-I", 7);
#[allow(dead_code)]
pub const NS_ISBN: NS<'static> = NS("ISBN", 9);

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
  #[allow(dead_code)]
  pub fn name(&'a self) -> &'a str {
    self.0
  }

  pub fn code(&'a self) -> i32 {
    self.1
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
