//! Code for working with genders.
use std::fmt;
use std::str::FromStr;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum GenderError {
    #[error("could not parse gender from string")]
    #[allow(dead_code)]
    ParseError,
}

/// A gender representation.
#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub enum Gender {
    Unknown,
    Ambiguous,
    Female,
    Male,
    Open(String),
}

/// A collection of genders.
#[derive(Default, Debug)]
pub struct GenderBag {
    size: usize,
    mask: u32,
    resolved: Gender,
}

impl Default for Gender {
    fn default() -> Self {
        Gender::Unknown
    }
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
            _ => Gender::Open(sg),
        }
    }
}

impl From<String> for Gender {
    fn from(s: String) -> Gender {
        s.as_str().into()
    }
}

impl<T> From<&T> for Gender
where
    T: Into<Gender> + Clone,
{
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
            Gender::Open(s) => f.write_str(s.as_str()),
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
            _ => Gender::Ambiguous,
        }
    }

    /// Get an integer usable as a mask for this gender.
    fn mask_val(&self) -> u32 {
        match self {
            Gender::Unknown => 1,
            Gender::Ambiguous => 2,
            Gender::Female => 4,
            Gender::Male => 8,
            Gender::Open(_) => 0x8000_0000,
        }
    }
}

impl GenderBag {
    /// Add a gender to this bag.
    pub fn add(&mut self, gender: Gender) {
        self.size += 1;
        self.mask |= gender.mask_val();
        self.resolved = self.resolved.merge(&gender);
    }

    /// Merge another gender bag into this one.
    pub fn merge_from(&mut self, bag: &GenderBag) {
        self.size += bag.size;
        self.mask |= bag.mask;
        self.resolved = self.resolved.merge(&bag.resolved);
    }

    /// Get the number of gender entries recorded to make this gender record.
    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.size
    }

    /// Check whether the gender bag is empty.
    pub fn is_empty(&self) -> bool {
        self.size == 0
    }

    /// Get the gender, returning [None] if no genders are seen.
    #[allow(dead_code)]
    pub fn maybe_gender(&self) -> Option<&Gender> {
        if self.is_empty() {
            None
        } else {
            Some(&self.resolved)
        }
    }

    /// Get the gender, returning [Gender::Unknown] if no genders are seen.
    pub fn to_gender(&self) -> &Gender {
        &self.resolved
    }
}

#[test]
pub fn test_bag_empty() {
    let bag = GenderBag::default();
    assert_eq!(bag.to_gender(), &Gender::Unknown);
}

#[test]
pub fn test_bag_female() {
    let mut bag = GenderBag::default();
    bag.add("female".into());
    assert_eq!(bag.to_gender(), &Gender::Female);
}

#[test]
pub fn test_bag_male() {
    let mut bag = GenderBag::default();
    bag.add("male".into());
    assert_eq!(bag.to_gender(), &Gender::Male);
}

#[test]
pub fn test_bag_mf() {
    let mut bag = GenderBag::default();
    bag.add("male".into());
    bag.add("female".into());
    assert_eq!(bag.to_gender(), &Gender::Ambiguous);
}

#[test]
pub fn test_bag_ff() {
    let mut bag = GenderBag::default();
    bag.add("female".into());
    bag.add("female".into());
    assert_eq!(bag.to_gender(), &Gender::Female);
}
