//! Utilities for working with the directory tree layout.
use std::collections::HashMap;
use std::env::current_dir;
use std::fs::read_to_string;
use std::io;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Result};
use log::*;
use text_template::{Piece, Template};

/// Configuration data.
pub struct Config {
    repr: toml::Value,
}

impl Config {
    pub fn load() -> Result<Config> {
        let mut path = find_path_root()?;
        path.push("config.toml");
        debug!("reading config from {:?}", path);
        let text = read_to_string(&path)?;
        Ok(Config {
            repr: toml::de::from_str(&text)?,
        })
    }

    /// Lookup a configuration option by dotted path.
    pub fn lookup_str<'a>(&'a self, path: &str) -> Option<&'a str> {
        let parts = path.split(".");
        let mut node = &self.repr;
        for part in parts {
            if let Some(n) = node.get(part) {
                node = n;
            } else {
                return None;
            }
        }

        node.as_str()
    }

    /// Interpolate a string.
    pub fn interpolate<'a, 'b: 'a>(&'a self, text: &'b str) -> String {
        let tmpl = Template::from(text);
        let mut vars = HashMap::new();
        for piece in tmpl.iter() {
            match piece {
                Piece::Placeholder { name, .. } => {
                    vars.insert(*name, self.lookup_str(name).unwrap_or_default());
                }
                _ => (),
            }
        }

        tmpl.fill_in(&vars).to_string()
    }
}

/// Check the TOML manifest to see if we're bookdata.
fn check_project_manifest(path: &Path) -> Result<bool> {
    let mut path = path.to_path_buf();
    path.push("Cargo.toml");

    let manifest = read_to_string(&path);
    match manifest {
        Ok(txt) => {
            let val: toml::Value = toml::de::from_str(&txt)?;
            if let Some(pkg) = val.get("package") {
                if let Some(name) = pkg.get("name") {
                    if name.as_str().unwrap_or_default() == "bookdata" {
                        Ok(true)
                    } else {
                        error!("Cargo.toml has package name {}", name);
                        Ok(false)
                    }
                } else {
                    error!("Cargo.toml has no name");
                    Ok(false)
                }
            } else {
                error!("Cargo.toml has no package section");
                Ok(false)
            }
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            error!("Cargo.toml not found alongside .dvc");
            Ok(false)
        }
        Err(e) => Err(e.into()),
    }
}

fn is_bookdata_root(path: &Path) -> Result<bool> {
    let mut dvc = path.to_path_buf();
    dvc.push(".dvc");
    if dvc.try_exists()? {
        debug!("found DVC path at {}", dvc.display());
        if check_project_manifest(path)? {
            Ok(true)
        } else {
            error!("found .dvc but not bookdata, running from a weird directory?");
            Err(anyhow!("bookdata not found"))
        }
    } else {
        Ok(false)
    }
}

/// Find the root path for the repository.
pub fn find_path_root() -> Result<PathBuf> {
    let mut path = current_dir()?;
    debug!("working directory: {}", path.display());
    loop {
        trace!("looking for DVC in {}", path.display());
        if is_bookdata_root(&path)? {
            info!("found bookdata root at {}", path.display());
            return Ok(path);
        }

        if !path.pop() {
            error!("scanned all parents and could not find bookdata root");
            return Err(anyhow!("bookdata not found"));
        }
    }
}

/// Require that we are in the root directory.
pub fn require_working_root() -> Result<()> {
    let cwd = current_dir()?;
    if is_bookdata_root(&cwd)? {
        Ok(())
    } else {
        error!("working directory is not bookdata root");
        Err(anyhow!("incorrect working directory"))
    }
}

/// Expect that we are in a particular subdirectory.
pub fn require_working_dir<P: AsRef<Path>>(path: P) -> Result<()> {
    let path = path.as_ref();
    let cwd = current_dir()?;

    if !cwd.ends_with(path) {
        error!("expected to be run in {:?}", path);
        Err(anyhow!("incorrect working directory"))
    } else {
        Ok(())
    }
}