//! Support for GoodReads user identifiers.
use anyhow::Result;
use log::*;

use crate::{ids::index::IdIndex, prelude::BDPath};

const GR_USER_FILE: BDPath<'static> = BDPath::new("goodreads/full/gr-users.parquet");
const UID_COL: &'static str = "user";
const UHASH_COL: &'static str = "user_hash";

pub type UserIndex = IdIndex<String>;

pub fn save_user_index(users: &UserIndex) -> Result<()> {
    let path = GR_USER_FILE.resolve()?;
    info!("saving {} users to {}", users.len(), path.display());
    users.save(&path, UID_COL, UHASH_COL)?;
    Ok(())
}

pub fn load_user_index() -> Result<UserIndex> {
    let path = GR_USER_FILE.resolve()?;
    let users = IdIndex::load(&path, UID_COL, UHASH_COL)?;
    info!("loaded {} users from {}", users.len(), path.display());
    Ok(users)
}
