//! Namespace maps.
use std::path::Path;
use std::fs;
use std::borrow::Cow;
use std::collections::HashMap;

use serde::{Deserialize, Deserializer};
use toml::from_str;

use anyhow::Result;

/// A namespace map for RDF contexts.
#[derive(Debug, Clone)]
pub struct NSMap {
  namespaces: Vec<(String, String)>
}

impl <'de> Deserialize<'de> for NSMap {
  fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
    let map: HashMap<String, String> = HashMap::deserialize(deserializer)?;
    let vec: Vec<(String, String)> = map.iter().map(|(s1,s2)| {
      (s1.clone(), s2.clone())
    }).collect();
    Ok(NSMap {
      namespaces: vec
    })
  }
}

impl NSMap {
  /// New empty namespace map.
  pub fn empty() -> NSMap {
    NSMap {
      namespaces: Vec::new()
    }
  }

  /// Load namespace definitions from a TOML file.
  pub fn load<P: AsRef<Path>>(path: P) -> Result<NSMap> {
    let text = fs::read_to_string(path)?;
    let map: NSMap = from_str(text.as_str())?;
    Ok(map)
  }

  /// Abbreviate a full URI using the namespace map.
  pub fn abbreviate_uri<'a>(&self, uri: &'a str) -> Cow<'a, str> {
    for (abbr, base) in &self.namespaces {
      if uri.starts_with(base) {
        let rest = &uri[base.len()..];
        return format!("{}:{}", abbr, rest).into()
      }
    }

    // no abbreviation found, skip
    uri.into()
  }
}
