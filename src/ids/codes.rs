pub struct NS<'a>(&'a str, i32);

const NS_MULT_BASE: i32 = 100000000;

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
