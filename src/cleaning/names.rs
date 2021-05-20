use regex::Regex;
use lazy_static::lazy_static;

lazy_static! {
  static ref NAME_LF_RE: Regex = Regex::new(r"(?x)
    ^
    # the surname
    ([^,]+)
    # comma separator
    ,\s*
    # the given name
    (.*?)
    # junk like a year
    (?:\s*[,.]?\s+\(?\d+\s*-.*)?
    $
  ").expect("bad RE");
  static ref TRIM_RE: Regex = Regex::new(r"^\W*(.*?)\W*$").expect("bad RE");
  static ref WS_RE: Regex = Regex::new(r"\s+").expect("bad RE");
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

#[cfg(test)]
fn check_name_decode(name: &str, exp_variants: &[&str]) {
  let dec_variants = name_variants(name);
  assert_eq!(dec_variants.len(), exp_variants.len());
  for n in exp_variants {
    assert!(dec_variants.contains(&(*n).to_owned()), "expected variant {} not found", n);
  }
}

#[test]
fn test_first_last() {
  check_name_decode("Mary Sumner", &["Mary Sumner"]);
}

#[test]
fn test_trim() {
  check_name_decode("Mary Sumner.", &["Mary Sumner"]);
}

#[test]
fn test_last_first_variants() {
  check_name_decode("Sequeira Moreno, Francisco", &[
    "Sequeira Moreno, Francisco",
    "Francisco Sequeira Moreno"
  ]);
}

#[test]
fn test_last_first_punctuation() {
  check_name_decode("Jomaa-Raad, Wafa,", &[
    "Wafa Jomaa-Raad",
    "Jomaa-Raad, Wafa",
  ]);
}

#[test]
#[ignore]
fn test_last_first_year() {
  check_name_decode("Morgan, Michelle, 1967-", &[
    "Morgan, Michelle, 1967-",
    "Morgan, Michelle",
    "Michelle Morgan"
  ]);
}

#[test]
#[ignore]
fn test_first_last_year() {
  check_name_decode("Ditlev Reventlow (1712-1783)", &[
    "Ditlev Reventlow (1712-1783)",
    "Ditlev Reventlow",
  ]);
}
