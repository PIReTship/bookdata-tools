//! Utilities for working with the directory tree layout.
use std::env::current_dir;
use std::fs::read_to_string;
use std::io;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Result};
use log::*;
use parse_display::Display;
use serde::Deserialize;

#[derive(Debug, Deserialize, Clone, Display)]
#[serde(rename_all = "kebab-case")]
#[display("kebab-case")]
pub enum GRInteractionMode {
    Simple,
    Full,
}

#[derive(Debug, Deserialize, Clone)]
pub struct DSConfig {
    pub enabled: bool,
}

#[derive(Debug, Deserialize, Clone)]
pub struct GRConfig {
    pub enabled: bool,
    pub interactions: GRInteractionMode,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub bx: DSConfig,
    pub az2014: DSConfig,
    pub az2018: DSConfig,
    pub goodreads: GRConfig,
}

impl Config {
    pub fn ds_enabled(&self, name: &str) -> bool {
        let (name, _qual) = if let Some((n, q)) = name.split_once("-") {
            (n, Some(q))
        } else {
            (name, None)
        };
        match name {
            "loc" | "LOC" => true,
            "openlib" | "openlibrary" | "OL" => true,
            "goodreads" | "GR" => self.goodreads.enabled,
            "az2014" | "AZ14" => self.az2014.enabled,
            "az2018" | "AZ18" => self.az2018.enabled,
            "bx" | "BX" => self.bx.enabled,
            _ => panic!("unsupported data set {}", name),
        }
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

/// Load the configuration for this project.
pub fn load_config() -> Result<Config> {
    let mut root = find_path_root()?;
    root.push("config.yaml");
    debug!("reading configuration {}", root.display());
    let f = read_to_string(&root)?;
    let cfg: Config = serde_yaml::from_str(&f)?;
    Ok(cfg)
}
