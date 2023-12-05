//! Support for GoodReads user identifiers.
use anyhow::Result;
use log::*;

use crate::ids::index::IdIndex;

const GR_USER_FILE: &str = "gr-users.parquet";
const UID_COL: &'static str = "user";
const UHASH_COL: &'static str = "user_hash";

pub type UserIndex = IdIndex<String>;

pub fn save_user_index(users: &UserIndex) -> Result<()> {
    info!("saving {} users", users.len());
    users.save(GR_USER_FILE, UID_COL, UHASH_COL)?;
    Ok(())
}

pub fn load_user_index() -> Result<UserIndex> {
    let users = IdIndex::load(GR_USER_FILE, UID_COL, UHASH_COL)?;
    info!("loaded {} users from {}", users.len(), GR_USER_FILE);
    Ok(users)
}
