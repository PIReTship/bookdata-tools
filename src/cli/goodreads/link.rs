use structopt::StructOpt;

use crate::prelude::*;
use crate::goodreads::*;
use crate::io::object::ThreadObjectWriter;
use crate::util::logging::data_progress;
use serde::de::DeserializeOwned;


#[derive(StructOpt, Debug)]
pub enum GRLink {
  /// Link ISBN IDs.
  #[structopt(name="isbn-ids")]
  IsbnIDs,
  /// Link book clusters.
  #[structopt(name="clusters")]
  Clusters,
}
