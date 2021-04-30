use std::path::PathBuf;
use std::time::Instant;

use log::*;

use glob::glob;
use structopt::StructOpt;
use fallible_iterator::FallibleIterator;
use happylog::set_progress;

use bookdata::prelude::*;
use bookdata::io::open_gzin_progress;

use bookdata::marc::MARCRecord;
use bookdata::marc::parse::{read_records, read_records_delim};

mod generic;
mod book;

/// Scan MARC book records and extract basic information.
#[derive(StructOpt, Debug)]
#[structopt(name="scan-marc")]
struct ParseMarcBooks {
  #[structopt(flatten)]
  common: CommonOpts,

  #[structopt(short="o", long="output", parse(from_os_str))]
  output: Option<PathBuf>,

  #[structopt(short="op", long="output-prefix")]
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

fn main() -> Result<()> {
  let opts = ParseMarcBooks::from_args();
  opts.common.init()?;

  // dispatch based on our operating mode
  if opts.book_mode {
    let pfx = match &opts.prefix {
      Some(p) => p,
      None => "book"
    };
    let output = book::BookOutput::open(pfx)?;
    opts.process_records(output)?;
  } else {
    let ofn = match &opts.output {
      Some(p) => p.clone(),
      None => PathBuf::from("marc-fields.parquet")
    };
    let output = generic::open_output(ofn)?;
    opts.process_records(output)?;
  };

  Ok(())
}

impl ParseMarcBooks {
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
      info!("reading from compressed file {:?}", inf);
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
      info!("processed {} records from {:?} in {:.2}s",
            nrecs, inf, file_start.elapsed().as_secs_f32());
      all_recs += nrecs;
    }

    let written = output.finish()?;

    info!("imported {}/{} records from {} files in {:.2}s",
          written, all_recs, nfiles, all_start.elapsed().as_secs_f32());

    Ok(())
  }
}
