//! Scan MARC records.  See [ScanMARC] for documentation.
use std::path::PathBuf;
use std::time::Instant;

use log::*;

use glob::glob;
use clap::Args;
use fallible_iterator::FallibleIterator;

use crate::prelude::*;
use crate::io::{open_gzin_progress, log_file_info};

use crate::marc::MARCRecord;
use crate::marc::parse::{read_records, read_records_delim};
use crate::marc::book_fields::BookOutput;
use crate::marc::flat_fields::FieldOutput;
use crate::util::logging::data_progress;

/// Scan MARC records and extract basic information.
///
/// This tool scans MARC-XML records, in either raw or delimited-line format,
/// and writes the fields to a Parquet file of flat field records.  It has two
/// modes: normal, which simply writes MARC fields to the Parquet file, and
/// 'book mode', which only saves books and produces additional output files
/// summarizing book record information and book ISBNs.
#[derive(Args, Debug)]
#[command(name="scan-marc")]
pub struct ScanMARC {
  /// Output files for normal mode.
  #[arg(short='o', long="output")]
  output: Option<PathBuf>,

  /// Prefix for output files in book mode.
  #[arg(short='p', long="output-prefix")]
  prefix: Option<String>,

  /// Turn on book mode.
  #[arg(long="book-mode")]
  book_mode: bool,

  /// Read in line mode
  #[arg(short='L', long="line-mode")]
  line_mode: bool,

  /// Glob for files to parse.
  #[arg(short='G', long="glob")]
  glob: Option<String>,

  /// Input files to parse (GZ-compressed)
  #[arg(name = "FILE")]
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
      let output = BookOutput::open(pfx)?;
      self.process_records(output)?;
    } else {
      let ofn = match &self.output {
        Some(p) => p.clone(),
        None => PathBuf::from("marc-fields.parquet")
      };
      let output = FieldOutput::open(&ofn)?;
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

  fn process_records<W: ObjectWriter<MARCRecord> + DataSink + Send + 'static>(&self, mut output: W) -> Result<()> {
    let mut nfiles = 0;
    let mut all_recs = 0;
    let all_start = Instant::now();

    for inf in self.find_files()? {
      nfiles += 1;
      let inf = inf.as_path();
      let file_start = Instant::now();
      info!("reading from compressed file {}", inf.display());
      let pb = data_progress(0);
      let read = open_gzin_progress(inf, pb.clone())?;
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

      info!("processed {} records from {} in {:.2}s",
            nrecs, inf.display(), file_start.elapsed().as_secs_f32());
      all_recs += nrecs;
    }

    let outs = output.output_files();
    let written = output.finish()?;

    info!("imported {} fields from {} records from {} files in {:.2}s",
          written, all_recs, nfiles, all_start.elapsed().as_secs_f32());
    log_file_info(&outs)?;

    Ok(())
  }
}
