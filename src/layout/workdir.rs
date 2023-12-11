use std::path::Path;
use std::{env::current_dir, path::PathBuf};

use anyhow::{anyhow, Result};
use log::*;
use relative_path::{RelativePath, RelativePathBuf};

fn is_bookdata_root(path: &Path) -> Result<bool> {
    let dvc = path.join(".dvc");
    let config = path.join("config.yaml");
    if dvc.try_exists()? {
        debug!("found DVC path at {}", dvc.display());
        if config.try_exists()? {
            debug!("found config.yaml at {}", config.display());
            Ok(true)
        } else {
            warn!("found .dvc but not config.yaml, weird directory?");
            Ok(false)
        }
    } else {
        Ok(false)
    }
}

/// Find a relative path from the current directory to the repository.
pub fn find_root_relpath() -> Result<RelativePathBuf> {
    let mut rp = RelativePathBuf::new();
    let cwd = current_dir()?;
    debug!("looking for root from working directory {}", cwd.display());
    loop {
        let path = rp.to_logical_path(&cwd);
        debug!("looking for DVC in {}", path.display());
        if is_bookdata_root(&path)? {
            info!("found bookdata root at {}", path.display());
            return Ok(rp);
        }

        if path.parent().is_some() {
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
    let resolved = rrp.to_logical_path(current_dir()?);
    debug!("resolved {} to {}", path.as_ref(), resolved.display());
    debug!("root: {}", root);
    debug!("rrp: {}", rrp);
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
