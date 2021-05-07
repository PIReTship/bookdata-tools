use chrono::prelude::*;
use super::dates::*;

#[test]
fn test_gr_date() {
  let src = "Tue Oct 03 10:28:16 -0700 2017";
  let date = parse_gr_date(src);
  let date = date.expect("date failed to parse");
  assert_eq!(date.year(), 2017);
  assert_eq!(date.month(), 10);
  assert_eq!(date.day(), 3);
}
