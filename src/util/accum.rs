/// Activatable data acumulator.
pub struct StringAccumulator {
    acc: String,
    active: bool,
}

impl StringAccumulator {
    /// Create a new data accumulator.
    pub fn new() -> StringAccumulator {
        StringAccumulator {
            acc: String::with_capacity(1024),
            active: false,
        }
    }

    /// Start accumulating data
    pub fn activate(&mut self) {
        self.active = true;
        self.acc.clear();
    }

    /// Add a slice of data to the accumulator.
    pub fn add_slice<S: AsRef<str>>(&mut self, other: S) {
        if self.active {
            self.acc.push_str(other.as_ref());
        }
    }

    pub fn finish(&mut self) -> &str {
        if self.active {
            self.active = false;
            &self.acc
        } else {
            ""
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
