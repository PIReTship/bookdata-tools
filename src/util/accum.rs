/// Activatable data acumulator.
pub struct DataAccumulator<T> {
  acc: Option<Vec<T>>
}

impl <T: Clone> DataAccumulator<T> {
  /// Create a new data accumulator.
  pub fn new() -> DataAccumulator<T> {
    DataAccumulator {
      acc: None
    }
  }

  /// Start accumulating data
  pub fn activate(&mut self) {
    self.acc = Some(Vec::new());
  }

  /// Finish the accumulator and return a vector
  pub fn finish(&mut self) -> Vec<T> {
    if let Some(vec) = self.acc.take() {
      vec
    } else {
      Vec::new()
    }
  }

  /// Add a slice of data to the accumulator.
  pub fn add_slice(&mut self, other: &[T]) {
    if let Some(vec) = self.acc.as_mut() {
      vec.extend_from_slice(other);
    }
  }
}

#[test]
fn test_empty() {
  let mut acc = DataAccumulator::new();
  let res: Vec<u8> = acc.finish();
  assert!(res.is_empty());
}

#[test]
fn test_add_inactive() {
  let mut acc = DataAccumulator::new();
  acc.add_slice("foo".as_bytes());
  let res: Vec<u8> = acc.finish();
  assert!(res.is_empty());
}

#[test]
fn test_accum() {
  let mut acc = DataAccumulator::new();
  acc.activate();
  acc.add_slice("lorem".as_bytes());
  acc.add_slice(" ipsum".as_bytes());
  let res: Vec<u8> = acc.finish();
  assert_eq!(String::from_utf8(res).expect("invalid utf8"), "lorem ipsum".to_owned());
}

#[test]
fn test_accum_finish() {
  let mut acc = DataAccumulator::new();
  acc.activate();
  acc.add_slice("lorem".as_bytes());
  acc.add_slice(" ipsum".as_bytes());
  let res: Vec<u8> = acc.finish();
  assert_eq!(String::from_utf8(res).expect("invalid utf8"), "lorem ipsum".to_owned());

  acc.add_slice("bob".as_bytes());
  let res: Vec<u8> = acc.finish();
  assert!(res.is_empty())
}

#[test]
fn test_accum_twice() {
  let mut acc = DataAccumulator::new();
  acc.activate();
  acc.add_slice("lorem".as_bytes());
  acc.add_slice(" ipsum".as_bytes());
  let res: Vec<u8> = acc.finish();
  assert_eq!(String::from_utf8(res).expect("invalid utf8"), "lorem ipsum".to_owned());

  acc.add_slice("bob".as_bytes());

  acc.activate();
  acc.add_slice("fishbone".as_bytes());
  let res: Vec<u8> = acc.finish();
  assert_eq!(String::from_utf8(res).expect("invalid utf8"), "fishbone".to_owned());
}
