use std::io::prelude::*;
use std::path::PathBuf;
use std::str;
use std::time::Instant;

use log::*;

use glob::glob;
use structopt::StructOpt;
use quick_xml::Reader;
use quick_xml::events::Event;
use anyhow::{Result, anyhow};
use happylog::set_progress;
use humantime::format_duration;

use bookdata::prelude::*;
use bookdata::io::open_gzin_progress;
use bookdata::parquet::*;
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
struct Record {
  rec_id: u32,
  fld_no: u32,
  tag: String,
  ind1: Option<String>,
  ind2: Option<String>,
  sf_code: Option<String>,
  contents: String
}

/// Process a tab-delimited line file.  VIAF provides their files in this format;
/// each line is a tab-separated pair of the VIAF ID and a single `record` instance.
fn process_delim_file<R: BufRead>(r: &mut R, w: &mut TableWriter<Record>, init: u32) -> Result<u32> {
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
fn process_marc_file<R: BufRead>(r: &mut R, w: &mut TableWriter<Record>, init: u32) -> Result<u32> {
  let mut parse = Reader::from_reader(r);
  let count = process_records(&mut parse, w, init)?;
  Ok(count)
}

fn process_records<B: BufRead>(rdr: &mut Reader<B>, writer: &mut TableWriter<Record>, start: u32) -> Result<u32> {
  let mut buf = Vec::new();
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
            record.contents = String::from_utf8(content.clone())?;
            writer.write(&record)?;
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

  info!("preparing to write {:?}", &opts.output);
  let mut writer = TableWriter::open(&opts.output)?;
  let mut count = 0;
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
      process_delim_file(&mut read, &mut writer, count)
    } else {
      process_marc_file(&mut read, &mut writer, count)
    };
    drop(read);
    if pb.position() != pb.length() {
      warn!("advanced {}, expected {}", pb.position(), pb.length());
    }
    pb.finish_and_clear();
    match nrecs {
      Ok(n) => {
        info!("processed {} records from {:?} in {}",
              n, inf, format_duration(file_start.elapsed()));
        count += n;
      },
      Err(e) => {
        error!("error in {:?}: {}", inf, e);
        return Err(e)
      }
    }
  }

  writer.finish()?;

  info!("imported {} records from {} files in {}",
        count, nfiles, format_duration(all_start.elapsed()));

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
