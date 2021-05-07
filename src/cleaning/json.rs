/// Some of our JSON objects have \u0000, which PostgreSQL doesn't like.
/// Clean those out while we import.
/// This uses a reusable buffer to reduce allocations.  The contents of
/// `buf` are erased, and at exit contain the cleaned JSON.
///
/// ```
/// use bookdata::cleaning::clean_json;
/// let mut buf = String::new();
/// clean_json("some bad text\\u0000 continued", &mut buf);
/// assert_eq!(buf, "some bad text continued");
/// ```
pub fn clean_json(json: &str, buf: &mut String) {
  let bad = "\\u0000";
  buf.clear();
  let mut cur = json;
  loop {
    match cur.find(bad) {
      None => {
        buf.push_str(cur);
        break
      },
      Some(i) => {
        buf.push_str(&cur[0..i]);
        cur = &cur[(i + bad.len())..]
      }
    }
  }
}

#[test]
fn clean_empty_is_empty() {
  let mut buf = String::new();
  clean_json("", &mut buf);
  assert_eq!(buf, "");
}

#[test]
fn clean_only_is_empty() {
  let mut buf = String::new();
  clean_json("\\u0000", &mut buf);
  assert_eq!(buf, "");
}

#[test]
fn clean_trail() {
  let mut buf = String::new();
  clean_json("bob\\u0000", &mut buf);
  assert_eq!(buf, "bob");
}

#[test]
fn clean_lead() {
  let mut buf = String::new();
  clean_json("\\u0000wombat", &mut buf);
  assert_eq!(buf, "wombat");
}

#[test]
fn clean_middle() {
  let mut buf = String::new();
  clean_json("heffalump\\u0000wumpus", &mut buf);
  assert_eq!(buf, "heffalumpwumpus");
}

#[test]
fn clean_multi() {
  let mut buf = String::new();
  clean_json("bob\\u0000dylan\\u0000fish", &mut buf);
  assert_eq!(buf, "bobdylanfish");
}


#[test]
fn clean_reuse_buffer() {
  let mut buf = String::new();
  clean_json("bob\\u0000dylan\\u0000fish", &mut buf);
  assert_eq!(buf, "bobdylanfish");
  clean_json("pizza fish", &mut buf);
  assert_eq!(buf, "pizza fish");
}
