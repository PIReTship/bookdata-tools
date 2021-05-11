use regex::Regex;
use lazy_static::lazy_static;

lazy_static! {
  static ref NAME_LF_RE: Regex = Regex::new(r"^([^,]+),\s*(.*)").expect("bad RE");
  static ref TRIM_RE: Regex = Regex::new(r"^\W*(.*?)\W*$").expect("bad RE");
}

pub fn name_variants(name: &str) -> Vec<String> {
  let name = TRIM_RE.replace(name, "$1");
  let mut variants = vec![name.to_string()];
  let long = NAME_LF_RE.replace(&name, "$2 $1");
  if long != name {
    variants.push(long.to_string());
  }
  variants
}

#[test]
fn test_first_last() {
  let name = "Mary Sumner";
  let vars = name_variants(name);
  assert_eq!(vars.len(), 1);
  assert_eq!(vars[0].as_str(), name);
}

#[test]
fn test_trim() {
  let name = "Mary Sumner.";
  let vars = name_variants(name);
  assert_eq!(vars.len(), 1);
  assert_eq!(vars[0].as_str(), "Mary Sumner");
}

#[test]
fn test_last_first_variants() {
  let name = "Sequeira Moreno, Francisco";
  let vars = name_variants(name);
  assert_eq!(vars.len(), 2);
  assert!(vars.contains(&name.to_owned()));
  assert!(vars.contains(&"Francisco Sequeira Moreno".to_owned()));
}
