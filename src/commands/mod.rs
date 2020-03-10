mod support;

pub use support::{CmdEntry, Command};

pub mod import_json;
pub mod import_ntriples;
pub mod make_uuid;
pub mod parse_marc;
pub mod pcat;
pub mod hash;
pub mod info;

pub fn commands<'a>() -> Vec<CmdEntry<'a>> {
  vec![
    pcat::PCat::get_entry(),
    make_uuid::MakeUuid::get_entry(),
    import_ntriples::ImportNtriples::get_entry(),
    import_json::ImportJson::get_entry(),
    parse_marc::ParseMarc::get_entry(),
    hash::Hash::get_entry(),
    info::Info::get_entry()
  ]
}
