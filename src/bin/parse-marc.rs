#[macro_use] extern crate log;

extern crate structopt;
extern crate quick_xml;
extern crate flate2;
extern crate indicatif;
extern crate bookdata;

use std::io::prelude::*;
use std::io::{self, BufReader};
use std::fs::File;
use std::path::PathBuf;
use std::str;
use structopt::StructOpt;
use quick_xml::Reader;
use quick_xml::events::Event;
use flate2::bufread::MultiGzDecoder;
use indicatif::{ProgressBar, ProgressStyle};

use bookdata::{Result, err, LogOpts};
use bookdata::cleaning::write_pgencoded;
use bookdata::tsv::split_first;
use bookdata::db::{DbOpts, copy_target};

/// Parse MARC files into records for a PostgreSQL table.
#[derive(StructOpt, Debug)]
#[structopt(name="parse-marc")]
struct Opt {
  #[structopt(flatten)]
  logging: LogOpts,

  #[structopt(flatten)]
  db: DbOpts,

  #[structopt(short="-t", long="table")]
  table: String,

  /// Activate line mode, e.g. for VIAF
  #[structopt(short="L", long="line-mode")]
  linemode: bool,
  /// Input files to parse (GZ-compressed)
  #[structopt(name = "FILE", parse(from_os_str))]
  files: Vec<PathBuf>
}

struct Field<'a> {
  ind1: &'a [u8],
  ind2: &'a [u8],
  code: &'a [u8]
}

/// Process a tab-delimited line file.  VIAF provides their files in this format;
/// each line is a tab-separated pair of the VIAF ID and a single `record` instance.
fn process_delim_file<R: BufRead, W: Write>(r: &mut R, w: &mut W, init: usize) -> Result<usize> {
  let mut rec_count = 0;
  for line in r.lines() {
    let lstr = line?;
    let (_id, xml) = split_first(&lstr).ok_or(err("invalid line"))?;
    let mut parse = Reader::from_str(xml);
    let n = process_records(&mut parse, w, init + rec_count)?;
    // we should only have one record per file
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

fn write_codes<W: Write>(w: &mut W, rno: usize, fno: i32, tag: &[u8], fld: Option<&Field>) -> Result<()> {
  let ids = format!("{}\t{}\t", rno, fno);
  w.write_all(ids.as_str().as_bytes())?;
  w.write_all(tag)?;
  w.write_all(b"\t")?;
  match fld {
    Some(f) => {
      w.write_all(f.ind1)?;
      w.write_all(b"\t")?;
      w.write_all(f.ind2)?;
      w.write_all(b"\t")?;
      w.write_all(f.code)?;
      w.write_all(b"\t")?;
    },
    None => {
      w.write_all(b"\\N\t\\N\t\\N\t")?;
    }
  }
  Ok(())
}

fn write_nl<W: Write>(w: &mut W) -> io::Result<()> {
  w.write_all(b"\n")
}

fn process_records<B: BufRead, W: Write>(rdr: &mut Reader<B>, out: &mut W, start: usize) -> Result<usize> {
  let mut buf = Vec::new();
  let mut output = false;
  let mut fno = 0;
  let mut tag = Vec::with_capacity(5);
  let mut ind1 = Vec::with_capacity(10);
  let mut ind2 = Vec::with_capacity(10);
  let mut recid = start;
  loop {
    match rdr.read_event(&mut buf)? {
      Event::Start(ref e) => {
        let name = str::from_utf8(e.local_name())?;
        match name {
          "record" => {
            recid += 1
          },
          "leader" => {
            write_codes(out, recid, fno, b"LDR", None)?;
            output = true;
          },
          "controlfield" => {
            fno += 1;
            let mut done = false;
            for ar in e.attributes() {
              let a = ar?;
              if a.key == b"tag" {
                let tag = a.unescaped_value()?;
                write_codes(out, recid, fno, &tag, None)?;
                done = true;
              }
            }
            assert!(done, "no tag found for control field");
            output = true;
          },
          "datafield" => {
            fno += 1;
            for ar in e.attributes() {
              let a = ar?;
              let v = a.unescaped_value()?;
              match a.key {
                b"tag" => tag.extend_from_slice(&*v),
                b"ind1" => ind1.extend_from_slice(&*v),
                b"ind2" => ind2.extend_from_slice(&*v),
                _ => ()
              }
            }
            assert!(tag.len() > 0, "no tag found for data field");
            assert!(ind1.len() > 0, "no ind1 found for data field");
            assert!(ind2.len() > 0, "no ind2 found for data field");
          },
          "subfield" => {
            let mut done = false;
            for ar in e.attributes() {
              let a = ar?;
              if a.key == b"code" {
                let code = a.unescaped_value()?;
                let field = Field { ind1: &ind1, ind2: &ind2, code: &code };
                write_codes(out, recid, fno, &tag, Some(&field))?;
                done = true;
              }
            }
            assert!(done, "no code found for subfield");
            output = true;
          }
          _ => ()
        }
      },
      Event::End(ref e) => {
        let name = str::from_utf8(e.local_name())?;
        match name {
          "leader" | "controlfield" | "subfield" => {
            write_nl(out)?;
            output =  false;
          },
          "datafield" => {
            tag.clear();
            ind1.clear();
            ind2.clear();
          },
          _ => ()
        }
      },
      Event::Text(e) => {
        if output {
          let t = e.unescaped()?;
          write_pgencoded(out, &t)?
        }
      },
      Event::Eof => break,
      _ => ()
    }
  }
  Ok(recid - start)
}

fn main() -> Result<()> {
  let opt = Opt::from_args();
  opt.logging.init()?;
  
  let query = format!("COPY {}.{} FROM STDIN", opt.db.schema(), opt.table);
  let mut out = copy_target(&opt.db, &query, "marc-field")?;

  let mut count = 0;

  for inf in opt.files {
    let inf = inf.as_path();
    info!("reading from compressed file {:?}", inf);
    let fs = File::open(inf)?;
    let pb = ProgressBar::new(fs.metadata()?.len());
    pb.set_style(ProgressStyle::default_bar().template("{elapsed_precise} {bar} {percent}% {bytes}/{total_bytes} (eta: {eta})"));
    let pbr = pb.wrap_read(fs);
    let pbr = BufReader::new(pbr);
    let gzf = MultiGzDecoder::new(pbr);
    let mut bfs = BufReader::new(gzf);
    let nrecs = if opt.linemode {
      process_delim_file(&mut bfs, &mut out, count)
    } else {
      process_marc_file(&mut bfs, &mut out, count)
    };
    match nrecs {
      Ok(n) => {
        info!("processed {} records from {:?}", n, inf);
        count += n;
      },
      Err(e) => {
        error!("error in {:?}: {}", inf, e);
        return Err(e)
      }
    }
  }
  Ok(())
}
