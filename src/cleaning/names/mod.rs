//! Extract and normalize author names.
//!
//! Names in the book data — both in author records and in their references in
//! book records — come in a variety of formats.  This module is responsible for
//! expanding and normalizing those name formats to improve data linkability.
//! Some records also include a year or date range for the author's lifetime. We
//! normalize names as follows:
//!
//! - If the name is “Last, First”, we emit both “Last, First” and “First Last”
//!   variants.
//! - If the name has a year, we emit each variant both with and without the
//!   year.
//! - Leading and trailing junk is cleaned
//!
//! This maximizes our ability to match records across sources recording names
//! in different formats.
//!
//! [`name_variants`] is the primary entry point for using this module.  The
//! [`clean_name`] function provides cleanup utilities without parsing, for
//! emitting names from book records.

use anyhow::Result;

use super::strings::norm_unicode;

mod parse;
mod types;

#[cfg(test)]
mod test_cleaning;
#[cfg(test)]
mod test_variants;

pub use types::NameError;
use types::NameFmt;

pub use parse::parse_name_entry;

/// Pre-clean a string without copying.
///
/// Many strings don't need advanced cleaning. This method tries to pre-clean
/// a string.  If the string cannot be pre-cleaned, it returns None.
fn preclean<'a>(name: &'a str) -> Option<&'a str> {
    let name = name.trim();

    let mut ws_count = 0;
    for c in name.bytes() {
        if c == b'.' {
            return None;
        } else if c.is_ascii_whitespace() {
            ws_count += 1;
            if ws_count > 1 {
                return None;
            }
        } else {
            if c == b',' && ws_count > 0 {
                return None; // need cleaning of space and ,
            }
            ws_count = 0;
        }
    }

    Some(name)
}

/// Clean up a name from unnecessary special characters.
pub fn clean_name<'a>(name: &'a str) -> String {
    let name = norm_unicode(name);

    // fast path for pretty clean strings
    // directly copying a string is faster than our character-by-character copying,
    // probably due to simd, so it's worth scanning for a fast path.
    if let Some(pc) = preclean(&name) {
        return pc.to_string();
    }

    // we use a manually-coded state machine instead of REs for performance
    let mut res = Vec::with_capacity(name.len());
    let mut in_seq = false;
    for c in name.bytes() {
        if in_seq {
            if c.is_ascii_whitespace() || c == b'.' {
                // no-op
            } else if c == b',' || res.is_empty() {
                // emit the comma and proceed
                res.push(c);
                in_seq = false;
            } else {
                // collapse whitespace sequence and proceed
                res.push(b' ');
                res.push(c);
                in_seq = false;
            }
        } else {
            if c.is_ascii_whitespace() || c == b'.' {
                in_seq = true;
            } else {
                res.push(c);
            }
        }
    }
    unsafe {
        // since we have copied bytes, except for ASCII manipulations, this is safe
        String::from_utf8_unchecked(res)
    }
}

/// Extract all variants from a name.
///
/// See the [module documentation][self] for details on this parsing process.
pub fn name_variants(name: &str) -> Result<Vec<String>, NameError> {
    let parse = parse_name_entry(name)?;
    let mut variants = Vec::new();
    match parse.name.simplify() {
        NameFmt::Empty => (),
        NameFmt::Single(n) => variants.push(n),
        NameFmt::TwoPart(last, first) => {
            variants.push(format!("{} {}", first, last));
            variants.push(format!("{}, {}", last, first));
        }
    };

    // create a version with the year
    if let Some(y) = parse.year {
        for i in 0..variants.len() {
            variants.push(format!("{}, {}", variants[i], y));
        }
    }

    let mut variants: Vec<String> = variants.iter().map(|s| clean_name(s)).collect();
    variants.sort();
    variants.dedup();

    Ok(variants)
}
