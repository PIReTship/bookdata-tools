use std::fs::OpenOptions;
use std::path::PathBuf;
use std::time::Instant;

use log::*;

use glob::glob;
use serde::Serialize;
use structopt::StructOpt;
use fallible_iterator::FallibleIterator;
use flate2::Compression;
use happylog::set_progress;

use bookdata::prelude::*;
use bookdata::io::open_gzin_progress;
use bookdata::io::object::ThreadWriter;
use bookdata::parquet::*;
use bookdata::marc::MARCRecord;
use bookdata::marc::parse::{read_records};
use bookdata::marc::flat_fields::FieldOutput;

/// Scan MARC book records and extract basic information.
#[derive(StructOpt, Debug)]
#[structopt(name="scan-marc")]
pub struct ParseMarcBooks {
  #[structopt(flatten)]
  common: CommonOpts,

  /// Prefix for output files
  #[structopt(short="p", long="output-prefix")]
  prefix: Option<String>,

  /// Glob for files to parse.
  #[structopt(short="G", long="glob")]
  glob: Option<String>,

  /// Input files to parse (GZ-compressed)
  #[structopt(name = "FILE", parse(from_os_str))]
  files: Vec<PathBuf>
}

#[derive(TableRow, Debug)]
struct BookIds {
  rec_id: u32,
  marc_cn: String,
  lccn: Option<String>,
  status: u8,
  rec_type: u8,
  bib_level: u8
}

#[derive(Serialize, Debug)]
struct RawISBN {
  rec_id: u32,
  isbn: String
}

struct BookOutput {
  n_books: u32,
  fields: FieldOutput,
  ids: TableWriter<BookIds>,
  isbns: ThreadWriter<RawISBN>
}

impl BookOutput {
  fn open(prefix: &str) -> Result<BookOutput> {
    let ffn = format!("{}-fields.parquet", prefix);
    info!("writing book fields to {}", ffn);
    let fields = TableWriter::open(ffn)?;
    let fields = FieldOutput::new(fields);

    let idfn = format!("{}-ids.parquet", prefix);
    info!("writing book IDs to {}", idfn);
    let ids = TableWriter::open(idfn)?;

    let isbnfn = format!("{}-isbns.csv.gz", prefix);
    info!("writing book IDs to {}", isbnfn);
    let isbns = OpenOptions::new().create(true).truncate(true).write(true).open(isbnfn)?;
    let isbns = flate2::write::GzEncoder::new(isbns, Compression::best());
    let isbns = csv::Writer::from_writer(isbns);
    let isbns = ThreadWriter::new(isbns);

    Ok(BookOutput {
      n_books: 0,
      fields, ids, isbns
    })
  }
}

impl ObjectWriter<MARCRecord> for BookOutput {
  fn write_object(&mut self, record: MARCRecord) -> Result<()> {
    self.n_books += 1;
    let rec_id = self.n_books;

    // scan for ISBNs
    for df in &record.fields {
      if df.tag == 20 {
        for sf in &df.subfields {
          if sf.code == b'a' {
            let isbn = RawISBN {
              rec_id, isbn: sf.content.clone()
            };
            self.isbns.write_object(isbn)?;
          }
        }
      }
    }

    // emit book IDs
    let ids = BookIds {
      rec_id,
      marc_cn: record.marc_control().ok_or_else(|| {
        anyhow!("no MARC control number")
      })?.to_owned(),
      lccn: record.lccn().map(|s| s.to_owned()),
      status: record.rec_status().unwrap_or(0),
      rec_type: record.rec_type().unwrap_or(0),
      bib_level: record.rec_bib_level().unwrap_or(0)
    };
    self.ids.write_object(ids)?;

    self.fields.write_object(record)?;
    Ok(())
  }

  fn finish(self) -> Result<usize> {
    self.fields.finish()?;
    self.ids.finish()?;
    self.isbns.finish()?;
    Ok(self.n_books as usize)
  }
}

fn main() -> Result<()> {
  let opts = ParseMarcBooks::from_args();
  opts.common.init()?;

  let pfx = match &opts.prefix {
    Some(p) => p,
    None => "book"
  };
  let mut output = BookOutput::open(&pfx)?;
  let mut nfiles = 0;
  let mut all_recs = 0;
  let all_start = Instant::now();

  for inf in opts.find_files()? {
    nfiles += 1;
    let inf = inf.as_path();
    let file_start = Instant::now();
    info!("reading from compressed file {:?}", inf);
    let (read, pb) = open_gzin_progress(inf)?;
    let _pbl = set_progress(&pb);
    let mut records = read_records(read);

    let mut nrecs = 0;
    while let Some(rec) = records.next()? {
      if rec.is_book() {
        output.write_object(rec)?;
      }
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
}
