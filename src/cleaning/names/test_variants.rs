//! Test name variant expansion

use super::types::*;
use super::parse_name_entry;
use super::name_variants;

fn check_name_decode(name: &str, exp_variants: &[&str]) {
  let dec_variants = name_variants(name).expect("parse error");
  println!("scanned name {}:", name);
  for v in &dec_variants {
    println!("- {}", v);
  }
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
fn test_last_first_year() {
  check_name_decode("Morgan, Michelle, 1967-", &[
    "Morgan, Michelle, 1967-",
    "Morgan, Michelle",
    "Michelle Morgan",
    "Michelle Morgan, 1967-"
  ]);
}

#[test]
fn test_first_last_year() {
  check_name_decode("Ditlev Reventlow (1712-1783)", &[
    "Ditlev Reventlow, 1712-1783",
    "Ditlev Reventlow",
  ]);
}

#[test]
fn test_trailing_comma() {
  check_name_decode("Miller, Pat Zietlow,", &[
    "Pat Zietlow Miller",
    "Miller, Pat Zietlow",
  ]);
}

#[test]
fn test_trailing_punctuation() {
  check_name_decode("Miller, Pat Zietlow,.", &[
    "Pat Zietlow Miller",
    "Miller, Pat Zietlow",
  ]);
}

#[test]
fn test_single_trailing() {
  let parse = parse_name_entry("Manopoly,").expect("parse error");
  assert!(parse.year.is_none());
  assert_eq!(parse.name, NameFmt::Single("Manopoly".to_string()));
  check_name_decode("Manopoly,", &["Manopoly"]);
}

#[test]
fn test_locked() {
  check_name_decode("!!!GESPERRT!!!Moro, Simone", &[
    "Simone Moro",
    "Moro, Simone"
  ]);
}

#[test]
fn test_single_initial() {
  check_name_decode("Navarro, P.", &[
    "Navarro, P",
    "P Navarro",
  ])
}

#[test]
fn test_leading_comma() {
  check_name_decode(", Engelbert", &[
    "Engelbert"
  ]);
}

#[test]
fn test_year_only() {
  check_name_decode("1941-", &[]);
}
