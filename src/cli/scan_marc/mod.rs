//! Scan MARC records.  See [ScanMARC] for documentation.
use std::path::PathBuf;
use std::time::Instant;

use log::*;

use glob::glob;
use structopt::StructOpt;
use fallible_iterator::FallibleIterator;
use happylog::set_progress;

use crate::prelude::*;
use crate::io::open_gzin_progress;

use crate::marc::MARCRecord;
use crate::marc::parse::{read_records, read_records_delim};

mod generic;
mod book;

/// Scan MARC records and extract basic information.
///
/// This tool scans MARC-XML records, in either raw or delimited-line format,
/// and writes the fields to a Parquet file of flat field records.  It has two
/// modes: normal, which simply writes MARC fields to the Parquet file, and
/// 'book mode', which only saves books and produces additional output files
/// summarizing book record information and book ISBNs.
#[derive(StructOpt, Debug)]
#[structopt(name="scan-marc")]
pub struct ScanMARC {
  #[structopt(flatten)]
  common: CommonOpts,

  /// Output files for normal mode.
  #[structopt(short="o", long="output", parse(from_os_str))]
  output: Option<PathBuf>,

  /// Prefix for output files in book mode.
  #[structopt(short="p", long="output-prefix")]
  prefix: Option<String>,

  /// Turn on book mode.
  #[structopt(long="book-mode")]
  book_mode: bool,

  /// Read in line mode
  #[structopt(short="L", long="line-mode")]
  line_mode: bool,

  /// Glob for files to parse.
  #[structopt(short="G", long="glob")]
  glob: Option<String>,

  /// Input files to parse (GZ-compressed)
  #[structopt(name = "FILE", parse(from_os_str))]
  files: Vec<PathBuf>
}

impl Command for ScanMARC {
  fn exec(&self) -> Result<()> {
    // dispatch based on our operating mode
    if self.book_mode {
      let pfx = match &self.prefix {
        Some(p) => p,
        None => "book"
      };
      let output = book::BookOutput::open(pfx)?;
      self.process_records(output)?;
    } else {
      let ofn = match &self.output {
        Some(p) => p.clone(),
        None => PathBuf::from("marc-fields.parquet")
      };
      let output = generic::open_output(ofn)?;
      self.process_records(output)?;
    };

    Ok(())
  }
}

impl ScanMARC {
  fn find_files(&self) -> Result<Vec<PathBuf>> {
    if let Some(ref gs) = self.glob {
      info!("scanning for files {}", gs);
      let mut v = Vec::new();
      for entry in glob(gs)? {
        let entry = entry?;
        v.push(entry);
      }
      Ok(v)
    } else {
      Ok(self.files.clone())
    }
  }

  fn process_records<W: ObjectWriter<MARCRecord>>(&self, output: W) -> Result<()> {
    let mut output = output;
    let mut nfiles = 0;
    let mut all_recs = 0;
    let all_start = Instant::now();

    for inf in self.find_files()? {
      nfiles += 1;
      let inf = inf.as_path();
      let file_start = Instant::now();
      info!("reading from compressed file {}", inf.display());
      let (read, pb) = open_gzin_progress(inf)?;
      let _pbl = set_progress(&pb);
      let mut records = if self.line_mode {
        read_records_delim(read)
      } else {
        read_records(read)
      };

      let mut nrecs = 0;
      while let Some(rec) = records.next()? {
        output.write_object(rec)?;
        nrecs += 1;
      }

      pb.finish_and_clear();
      info!("processed {} records from {} in {:.2}s",
            nrecs, inf.display(), file_start.elapsed().as_secs_f32());
      all_recs += nrecs;
    }

    let written = output.finish()?;

    info!("imported {}/{} records from {} files in {:.2}s",
          written, all_recs, nfiles, all_start.elapsed().as_secs_f32());

    Ok(())
  }
}
