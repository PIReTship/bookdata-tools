/// Activatable data acumulator.
pub struct StringAccumulator {
  acc: Option<String>
}

impl StringAccumulator {
  /// Create a new data accumulator.
  pub fn new() -> StringAccumulator {
    StringAccumulator {
      acc: None
    }
  }

  /// Start accumulating data
  pub fn activate(&mut self) {
    self.acc = Some(String::with_capacity(1024));
  }

  /// Add a slice of data to the accumulator.
  pub fn add_slice<S: AsRef<str>>(&mut self, other: S) {
    if let Some(string) = self.acc.as_mut() {
      string.push_str(other.as_ref());
    }
  }

  pub fn finish(&mut self) -> String {
    if let Some(string) = self.acc.take() {
      string
    } else {
      String::new()
    }
  }
}

#[test]
fn test_empty() {
  let mut acc = StringAccumulator::new();
  let res = acc.finish();
  assert!(res.is_empty());
}

#[test]
fn test_add_inactive() {
  let mut acc = StringAccumulator::new();
  acc.add_slice("foo");
  let res = acc.finish();
  assert!(res.is_empty());
}

#[test]
fn test_accum() {
  let mut acc = StringAccumulator::new();
  acc.activate();
  acc.add_slice("lorem");
  acc.add_slice(" ipsum");
  let res = acc.finish();
  assert_eq!(res, "lorem ipsum".to_owned());
}

#[test]
fn test_accum_finish() {
  let mut acc = StringAccumulator::new();
  acc.activate();
  acc.add_slice("lorem");
  acc.add_slice(" ipsum");
  let res = acc.finish();
  assert_eq!(res, "lorem ipsum".to_owned());

  acc.add_slice("bob");
  let res = acc.finish();
  assert!(res.is_empty())
}

#[test]
fn test_accum_twice() {
  let mut acc = StringAccumulator::new();
  acc.activate();
  acc.add_slice("lorem");
  acc.add_slice(" ipsum");
  let res = acc.finish();
  assert_eq!(res, "lorem ipsum".to_owned());

  acc.add_slice("bob");

  acc.activate();
  acc.add_slice("fishbone");
  let res = acc.finish();
  assert_eq!(res, "fishbone".to_owned());
}
