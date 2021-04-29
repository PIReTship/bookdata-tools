use std::io::prelude::*;
use std::path::PathBuf;
use std::time::Instant;

use log::*;

use glob::glob;
use structopt::StructOpt;
use quick_xml::Reader;
use anyhow::{Result, anyhow};
use fallible_iterator::FallibleIterator;
use happylog::set_progress;

use bookdata::prelude::*;
use bookdata::io::open_gzin_progress;
use bookdata::parquet::*;
use bookdata::marc::MARCRecord;
use bookdata::tsv::split_first;

/// Parse MARC files Parquet tables.
#[derive(StructOpt, Debug)]
#[structopt(name="scan-marc")]
pub struct ParseMarc {
  #[structopt(flatten)]
  common: CommonOpts,

  /// Output file
  #[structopt(short="o", long="output")]
  output: PathBuf,

  /// Activate line mode, e.g. for VIAF
  #[structopt(short="L", long="line-mode")]
  linemode: bool,

  #[structopt(long="src-dir")]
  src_dir: Option<PathBuf>,

  #[structopt(long="src-prefix")]
  src_prefix: Option<String>,

  /// Input files to parse (GZ-compressed)
  #[structopt(name = "FILE", parse(from_os_str))]
  files: Vec<PathBuf>
}

#[derive(TableRow, Debug, Default)]
struct FieldRecord {
  rec_id: u32,
  fld_no: u32,
  tag: i16,
  ind1: u8,
  ind2: u8,
  sf_code: u8,
  contents: String
}

struct Output {
  rec_count: u32,
  writer: TableWriter<FieldRecord>
}

impl Output {
  fn new(writer: TableWriter<FieldRecord>) -> Output {
    Output {
      rec_count: 0,
      writer
    }
  }

  fn write_marc_record(&mut self, rec: MARCRecord) -> Result<()> {
    self.rec_count += 1;
    let rec_id = self.rec_count;
    let mut fld_no = 0;

    // write the leader
    self.writer.write(&FieldRecord {
      rec_id, fld_no,
      tag: -1,
      ind1: 0, ind2: 0, sf_code: 0,
      contents: rec.leader
    })?;

    // write the control fields
    for cf in rec.control {
      fld_no += 1;
      self.writer.write(&FieldRecord {
        rec_id, fld_no, tag: cf.tag.into(),
        ind1: 0, ind2: 0, sf_code: 0,
        contents: cf.content
      })?;
    }

    // write the data fields
    for df in rec.fields {
      for sf in df.subfields {
        fld_no += 1;
        self.writer.write(&FieldRecord {
          rec_id, fld_no,
          tag: df.tag, ind1: df.ind1, ind2: df.ind2,
          sf_code: sf.code,
          contents: sf.content
        })?;
      }
    }

    Ok(())
  }
}

/// Process a tab-delimited line file.  VIAF provides their files in this format;
/// each line is a tab-separated pair of the VIAF ID and a single `record` instance.
fn process_delim_file<R: BufRead>(r: &mut R, w: &mut Output) -> Result<usize> {
  let mut n = 0;
  for line in r.lines() {
    n += 1;
    let lstr = line?;
    let (_id, xml) = split_first(&lstr).ok_or(anyhow!("invalid line"))?;
    let rec = MARCRecord::parse_record(&xml)?;
    w.write_marc_record(rec)?;
  }
  Ok(n)
}

/// Process a file containing a MARC collection.
fn process_marc_file<R: BufRead>(r: &mut R, w: &mut Output) -> Result<usize> {
  let mut n = 0;
  let mut parse = Reader::from_reader(r);
  let mut records = MARCRecord::read_records(&mut parse);
  while let Some(rec) = records.next()? {
    n += 1;
    w.write_marc_record(rec)?
  }
  Ok(n)
}

fn main() -> Result<()> {
  let opts = ParseMarc::from_args();
  opts.common.init()?;

  info!("preparing to write {:?}", &opts.output);
  let writer = TableWriter::open(&opts.output)?;
  let mut writer = Output::new(writer);
  let mut nfiles = 0;
  let all_start = Instant::now();

  for inf in opts.find_files()? {
    nfiles += 1;
    let inf = inf.as_path();
    let file_start = Instant::now();
    info!("reading from compressed file {:?}", inf);
    let (read, pb) = open_gzin_progress(inf)?;
    let _pbl = set_progress(&pb);
    let mut read = read;
    let nrecs = if opts.linemode {
      process_delim_file(&mut read, &mut writer)
    } else {
      process_marc_file(&mut read, &mut writer)
    };
    drop(read);
    if pb.position() != pb.length() {
      warn!("advanced {}, expected {}", pb.position(), pb.length());
    }
    pb.finish_and_clear();
    match nrecs {
      Ok(n) => {
        info!("processed {} records from {:?} in {:.2}s",
              n, inf, file_start.elapsed().as_secs_f32());
      },
      Err(e) => {
        error!("error in {:?}: {}", inf, e);
        return Err(e)
      }
    }
  }

  let nrecs = writer.writer.finish()?;

  info!("imported {} records from {} files in {:.2}s",
        nrecs, nfiles, all_start.elapsed().as_secs_f32());

  Ok(())
}

impl ParseMarc {
  fn find_files(&self) -> Result<Vec<PathBuf>> {
    if let Some(ref dir) = self.src_dir {
      let mut ds = dir.to_str().unwrap().to_string();
      if let Some(ref pfx) = self.src_prefix {
        ds.push_str("/");
        ds.push_str(pfx);
        ds.push_str("*.xml.gz");
      } else {
        ds.push_str("/*.xml.gz");
      }
      info!("scanning for files {}", ds);
      let mut v = Vec::new();
      for entry in glob(&ds)? {
        let entry = entry?;
        v.push(entry);
      }
      Ok(v)
    } else {
      Ok(self.files.clone())
    }
  }
}
