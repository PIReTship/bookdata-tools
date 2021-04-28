use std::io::prelude::*;
use std::io::{self, BufReader, BufWriter};
use std::fs::File;
use std::path::PathBuf;
use std::str;

use log::*;

use sha1::Sha1;
use glob::glob;
use structopt::StructOpt;
use quick_xml::Reader;
use quick_xml::events::Event;
use flate2::bufread::MultiGzDecoder;
use indicatif::{ProgressBar, ProgressStyle};
use anyhow::{Result, anyhow};
use happylog::set_progress;

use bookdata::prelude::*;
use bookdata::parquet::*;
use crate::cleaning::write_pgencoded;
use crate::tsv::split_first;
use crate::tracking::StageOpts;
use crate::io::{HashWrite};
use crate::db::{DbOpts, CopyRequest};
use super::Command;

/// Parse MARC files Parquet tables.
#[derive(StructOpt, Debug)]
#[structopt(name="scan-marc")]
pub struct ParseMarc {
  #[structopt(flatten)]
  common: CommonOpts,

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
struct Record {
  rec_id: u32,
  fld_no: u32,
  tag: String,
  ind1: Option<String>,
  ind2: Option<String>,
  sf_code: Option<String>,
  contents: String
}

impl Record {
  fn new<S: AsRef<str>>(rec_id: u32, fld_no: u32, tag: S) -> Record {
    Record {
      rec_id, fld_no,
      tag: tag.as_ref().to_owned(),
      ind1: None, ind2: None, sf_code: None, contents: String::new()
    }
  }
}

/// Process a tab-delimited line file.  VIAF provides their files in this format;
/// each line is a tab-separated pair of the VIAF ID and a single `record` instance.
fn process_delim_file<R: BufRead, W: Write>(r: &mut R, w: &mut W, init: usize) -> Result<usize> {
  let mut rec_count = 0;
  for line in r.lines() {
    let lstr = line?;
    let (_id, xml) = split_first(&lstr).ok_or(anyhow!("invalid line"))?;
    let mut parse = Reader::from_str(xml);
    let n = process_records(&mut parse, w, init + rec_count)?;
    // we should only have one record per line
    assert_eq!(n, 1);
    rec_count += n;
  }

  Ok(rec_count)
}

/// Process a file containing a MARC collection.
fn process_marc_file<R: BufRead, W: Write>(r: &mut R, w: &mut W, init: usize) -> Result<usize> {
  let mut parse = Reader::from_reader(r);
  let count = process_records(&mut parse, w, init)?;
  Ok(count)
}

fn process_records<B: BufRead, W: Write>(rdr: &mut Reader<B>, out: &mut W, start: u32) -> Result<u32> {
  let mut buf = Vec::new();
  let mut output = false;
  let mut tag = Vec::with_capacity(5);
  let mut ind1 = Vec::with_capacity(10);
  let mut ind2 = Vec::with_capacity(10);
  let mut content = Vec::with_capacity(100);
  let mut record = Record::default();
  record.rec_id = start;
  loop {
    match rdr.read_event(&mut buf)? {
      Event::Start(ref e) => {
        let name = str::from_utf8(e.local_name())?;
        match name {
          "record" => {
            record.rec_id += 1;
            record.fld_no = 0;
          },
          "leader" => {
            record.tag = "LDR".to_owned();
            content.clear();
          },
          "controlfield" => {
            record.fld_no += 1;
            let mut ntags = 0;
            for ar in e.attributes() {
              let a = ar?;
              if a.key == b"tag" {
                let tag = a.unescaped_value()?;
                record.tag = str::from_utf8(&tag)?.to_owned();
                ntags += 1;
              }
            }
            assert!(ntags == 1, "no tag found for control field");
            content.clear();
          },
          "datafield" => {
            record.fld_no += 1;
            for ar in e.attributes() {
              let a = ar?;
              let v = a.unescaped_value()?;
              let v = str::from_utf8(&v)?;
              match a.key {
                b"tag" => record.tag = v.to_owned(),
                b"ind1" => record.ind1 = Some(v.to_owned()),
                b"ind2" => record.ind2 = Some(v.to_owned()),
                _ => ()
              }
            }
            assert!(tag.len() > 0, "no tag found for data field");
            assert!(ind1.len() > 0, "no ind1 found for data field");
            assert!(ind2.len() > 0, "no ind2 found for data field");
          },
          "subfield" => {
            let mut natts = 0;
            for ar in e.attributes() {
              let a = ar?;
              if a.key == b"code" {
                let code = a.unescaped_value()?;
                record.sf_code = Some(str::from_utf8(&code)?.to_owned());
                natts += 1;
              }
            }
            assert!(natts >= 1, "no code found for subfield");
            assert!(natts <= 1, "too many codes found for subfield");
            content.clear();
          }
          _ => ()
        }
      },
      Event::End(ref e) => {
        let name = str::from_utf8(e.local_name())?;
        match name {
          "leader" | "controlfield" | "subfield" => {
            record.contents = String::from_utf8(content)?;
            writer.write(&record);
          },
          "datafield" => {
            record.tag.clear();
            record.ind1 = None;
            record.ind2 = None;
            record.sf_code = None;
            record.contents.clear();
          },
          _ => ()
        }
      },
      Event::Text(e) => {
        let t = e.unescaped()?;
        content.extend_from_slice(&t);
      },
      Event::Eof => break,
      _ => ()
    }
  }
  Ok(record.rec_id - start)
}

fn main() -> Result<()> {
  let opts = ParseMarc::from_args();
  opts.common.init()?;

  let db = self.db.open()?;
  let req = CopyRequest::new(&self.db, &self.table)?;
  let req = req.with_schema(self.db.schema());
  let req = req.truncate(self.truncate);
  let out = req.open()?;
  let mut out_h = Sha1::new();
  let out = HashWrite::create(out, &mut out_h);
  let mut out = BufWriter::new(out);

  let mut stage = self.stage.begin_stage(&db)?;

  let mut count = 0;

  for inf in opts.find_files()? {
    let inf = inf.as_path();
    info!("reading from compressed file {:?}", inf);
    let fs = File::open(inf)?;
    let pb = ProgressBar::new(fs.metadata()?.len());
    pb.set_style(ProgressStyle::default_bar().template("{elapsed_precise} {bar} {percent}% {bytes}/{total_bytes} (eta: {eta})"));
    let _pbs = set_progress(&pb);
    let mut in_sf = stage.source_file(inf);
    let pbr = pb.wrap_read(fs);
    let pbr = BufReader::new(pbr);
    let gzf = MultiGzDecoder::new(pbr);
    let gzf = in_sf.wrap_read(gzf);
    let mut bfs = BufReader::new(gzf);
    let nrecs = if self.linemode {
      process_delim_file(&mut bfs, &mut out, count)
    } else {
      process_marc_file(&mut bfs, &mut out, count)
    };
    drop(bfs);
    match nrecs {
      Ok(n) => {
        info!("processed {} records from {:?}", n, inf);
        let hash = in_sf.record()?;
        writeln!(&mut stage, "READ {:?} {} {}", inf, n, hash)?;
        count += n;
      },
      Err(e) => {
        error!("error in {:?}: {}", inf, e);
        return Err(e)
      }
    }
  }

  drop(out);
  let out_h = out_h.hexdigest();
  writeln!(&mut stage, "COPY {}", out_h)?;
  stage.end(&Some(out_h))?;

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
