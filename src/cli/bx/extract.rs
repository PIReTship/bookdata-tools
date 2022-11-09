//! BookCrossing data extraction.
//!
//! The BookCrossing CSV files are corrupt, so this command extracts them and fixes
//! up the character sets to make them well-formed CSV.
use std::io::{Write};
use std::path::{PathBuf};
use std::fs::File;

use zip::ZipArchive;

use crate::prelude::*;

#[derive(Args, Debug)]
pub struct Extract {
  /// The zip file to read.
  #[arg(name="ZIPFILE")]
  zipfile: PathBuf,

  /// The CSV file to write.
  #[arg(name="OUTFILE")]
  outfile: PathBuf,
}

impl Command for Extract {
  fn exec(&self) -> Result<()> {
    info!("reading {:?}", self.zipfile);
    let file = File::open(&self.zipfile)?;
    let mut zip = ZipArchive::new(file)?;
    let mut entry = zip.by_name("BX-Book-Ratings.csv")?;
    let mut data = entry.read_all_sized()?;

    info!("cleaning up data file");

    debug!("removing non-ASCII characters and carriage returns");
    data.retain(|b| *b < 128 && *b != b'\r');

    debug!("replacing semicolons to make CSV");
    // can this be done with retain_with?
    for i in 0..data.len() {
      let c = data[i];
      if c == b';' {
        data[i] = b',';
      }
    }

    debug!("splitting CSV header");
    let data = String::from_utf8(data)?;
    let pos = if let Some(p) = data.find('\n') {
      p
    } else {
      error!("no newline found, corrupt input data?");
      return Err(anyhow!("corrupt data"));
    };
    let (hdr, rest) = data.split_at(pos + 1);
    if !hdr.starts_with("\"User-ID\",") {
      error!("unexpected file header found");
      info!("found header: “{}“", hdr);
      info!("expected cleaned header to begin with “\"User-ID\",“");
      return Err(anyhow!("corrupt data"));
    }

    info!("writing cleaned output");
    let mut out = File::create(&self.outfile)?;
    write!(out, "user,isbn,rating\n")?;
    let csvin = csv::Reader::from_reader(rest.as_bytes());
    for row in csvin.into_records() {
      let row = row?;
      let user = row.get(0).ok_or(anyhow!("invalid CSV row"))?;
      let isbn = row.get(1).ok_or(anyhow!("invalid CSV row"))?;
      let rating = row.get(2).ok_or(anyhow!("invalid CSV row"))?;

      let mut isbn = isbn.to_uppercase();
      isbn.retain(|c| (c >= '0' && c <= '9') || c == 'X');

      if isbn.len() > 0 {
        write!(out, "{},{},{}\n", user, isbn, rating)?;
      }
    }

    Ok(())
  }
}
