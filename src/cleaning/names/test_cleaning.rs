//! Test the [`clean_name`] function.

use super::clean_name;

#[test]
fn test_clean_point() {
  let name = "W. Seiler";
  let clean = clean_name(name);
  assert_eq!(&clean, "W Seiler");
}

#[test]
fn test_clean_spaces() {
  let name = "Zaphod  Beeblebrox";
  let clean = clean_name(name);
  assert_eq!(&clean, "Zaphod Beeblebrox");
}


#[test]
fn test_clean_final_dot() {
  let name = "Bob J.";
  let clean = clean_name(name);
  assert_eq!(&clean, "Bob J");
}

#[test]
fn test_clean_comma() {
  let name = "Jones Jr., Albert";
  let clean = clean_name(name);
  assert_eq!(&clean, "Jones Jr, Albert");
}


#[test]
fn test_trim() {
  let name: &str = "  Bob., Qbert. ";
  let clean = clean_name(name);
  assert_eq!(&clean, "Bob, Qbert");
}
