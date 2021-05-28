//! Utilities for cleaning strings.
use std::iter::FromIterator;

use std::borrow::Cow;
use unicode_normalization::*;

/// Normalize Unicode character representations in a string.
pub fn norm_unicode<'a>(s: &'a str) -> Cow<'a, str> {
  if is_nfd_quick(s.chars()) == IsNormalized::Yes {
    s.into()
  } else {
    String::from_iter(s.nfd()).into()
  }
}
