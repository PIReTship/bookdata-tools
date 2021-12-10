//! Code for working with genders.
use std::str::FromStr;
use std::fmt;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum GenderError {
  #[error("could not parse gender from string")]
  #[allow(dead_code)]
  ParseError
}

/// A gender representation.
#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub enum Gender {
  Unknown,
  Ambiguous,
  Female,
  Male,
  Open(String)
}

impl FromStr for Gender {
  type Err = GenderError;

  fn from_str(s: &str) -> Result<Gender, GenderError> {
    Ok(s.into())
  }
}

impl From<&str> for Gender {
  fn from(s: &str) -> Gender {
    let sg = s.trim().to_lowercase();
    match sg.as_str() {
      "unknown" => Gender::Unknown,
      "ambiguous" => Gender::Ambiguous,
      "female" => Gender::Female,
      "male" => Gender::Male,
      _ => Gender::Open(sg)
    }
  }
}

impl From<String> for Gender {
  fn from(s: String) -> Gender {
    s.as_str().into()
  }
}

impl <T> From<&T> for Gender where T: Into<Gender> + Clone {
  fn from(obj: &T) -> Gender {
    obj.clone().into()
  }
}

impl fmt::Display for Gender {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      Gender::Unknown => f.write_str("unknown"),
      Gender::Ambiguous => f.write_str("ambiguous"),
      Gender::Female => f.write_str("female"),
      Gender::Male => f.write_str("male"),
      Gender::Open(s) => f.write_str(s.as_str())
    }
  }
}

impl Gender {
  /// Merge a gender record with another.  If the two records
  /// disagree, the result is [Gender::Ambiguous].
  pub fn merge(&self, other: &Gender) -> Gender {
    match (self, other) {
      (Gender::Unknown, g) => g.clone(),
      (g, Gender::Unknown) => g.clone(),
      (g1, g2) if g1 == g2 => g2.clone(),
      _ => Gender::Ambiguous
    }
  }
}

pub fn resolve_gender<I,G>(genders: I) -> Gender
where I: IntoIterator<Item=G>,
      G: Into<Gender>
{
  let mut gender = Gender::Unknown;
  for g in genders {
    let g: Gender = g.into();
    gender = gender.merge(&g);
  }
  gender.to_owned()
}


#[test]
pub fn test_resolve_empty() {
  let g = resolve_gender(Vec::<String>::new());
  assert_eq!(g, Gender::Unknown);
}

#[test]
pub fn test_resolve_female() {
  let g = resolve_gender(vec!["female"]);
  assert_eq!(g, Gender::Female);
}

#[test]
pub fn test_resolve_male() {
  let g = resolve_gender(vec!["male"]);
  assert_eq!(g, Gender::Male);
}

#[test]
pub fn test_resolve_mf() {
  let g = resolve_gender(vec!["male", "female"]);
  assert_eq!(g, Gender::Ambiguous);
}

#[test]
pub fn test_resolve_f_unknown() {
  let g = resolve_gender(vec!["female", "unknown"]);
  assert_eq!(g, Gender::Female);
}

#[test]
pub fn test_resolve_u_f() {
  let g = resolve_gender(vec!["unknown", "female"]);
  assert_eq!(g, Gender::Female);
}
