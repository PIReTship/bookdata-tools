use std::fmt;
use std::{borrow::Cow, path::PathBuf};

use anyhow::Result;
use relative_path::RelativePath;

use super::workdir::resolve_path;

/// Paths to book data files (primarily for defining constants, etc.).
#[derive(Debug, Clone)]
pub struct BDPath<'a> {
    path: Cow<'a, str>,
}

impl<'a> BDPath<'a> {
    /// Create a new book data path.
    pub const fn new(path: &'a str) -> BDPath<'a> {
        BDPath {
            path: Cow::Borrowed(path),
        }
    }

    /// Resolve a path.
    pub fn resolve(&self) -> Result<PathBuf> {
        let path = resolve_path(&self)?;
        Ok(path)
    }
}

impl<'a, 'b> AsRef<RelativePath> for &'b BDPath<'a>
where
    'b: 'a,
{
    fn as_ref(&self) -> &'a RelativePath {
        RelativePath::new(self.path.as_ref())
    }
}

impl<'a> fmt::Display for BDPath<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.path.as_ref())
    }
}
