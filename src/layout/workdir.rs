use std::fs::read_to_string;
use std::io;
use std::path::Path;
use std::{env::current_dir, path::PathBuf};

use anyhow::{anyhow, Result};
use log::*;
use relative_path::{RelativePath, RelativePathBuf};

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

/// Find a relative path from the current directory to the repository.
pub fn find_root_relpath() -> Result<RelativePathBuf> {
    let mut rp = RelativePathBuf::new();
    let cwd = current_dir()?;
    debug!("working directory: {}", cwd.display());
    loop {
        let path = rp.to_path(&cwd);
        trace!("looking for DVC in {}", path.display());
        if is_bookdata_root(&path)? {
            info!("found bookdata root at {}", path.display());
            return Ok(rp);
        }

        if path.parent().is_none() {
            rp.push("..");
        } else {
            error!("scanned parents and could not find bookdata root");
            return Err(anyhow!("bookdata not found"));
        }
    }
}

/// Get the absolute path to the repository root.
pub fn find_root_abspath() -> Result<PathBuf> {
    Ok(find_root_relpath()?.to_path(current_dir()?))
}

/// Given a path relative to the GoodReads repository root,
/// obtain a path to open the file.
pub fn resolve_path<P: AsRef<RelativePath>>(path: P) -> Result<PathBuf> {
    let root = find_root_relpath()?;
    let rrp = root.join_normalized(path.as_ref());
    let resolved = rrp.to_logical_path(".");
    debug!("resolved {} to {}", path.as_ref(), resolved.display());
    Ok(resolved)
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
