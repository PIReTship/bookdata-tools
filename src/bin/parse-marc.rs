#[macro_use]
extern crate structopt;
extern crate quick_xml;
extern crate flate2;

use std::io::prelude::*;
use std::io::{self, BufReader};
use std::fs::File;
use std::path::PathBuf;
use std::str;
use structopt::StructOpt;
use quick_xml::Reader;
use quick_xml::events::Event;
use flate2::read::GzDecoder;

#[derive(StructOpt, Debug)]
#[structopt(name="parse-marc")]
struct Opt {
  #[structopt(name = "FILE", parse(from_os_str))]
  infile: Option<PathBuf>
}

struct Field<'a> {
  ind1: &'a [u8],
  ind2: &'a [u8],
  code: &'a [u8]
}

fn process_delim_file<R: BufRead, W: Write>(r: &mut R, w: &mut W) -> io::Result<i32> {
  let mut count = 0;
  for line in r.lines() {
    let lstr = line?;
    match lstr.find('\t') {
      Some(i) => {
        let (id, xml) = lstr.split_at(i);
        let mut parse = Reader::from_str(xml);
        process_record(&mut parse, w, &mut count);
      },
      None => {
        panic!("invalid line");
      }
    }
  }

  Ok(count)
}

fn write_codes<W: Write>(w: &mut W, rno: i32, fno: i32, tag: &[u8], fld: Option<&Field>) -> io::Result<()> {
  let ids = format!("{}\t{}\t", rno, fno);
  w.write_all(ids.as_str().as_bytes());
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

fn write_data<W: Write>(w: &mut W, buf: &[u8]) -> io::Result<()> {
  let mut start = 0;
  for i in 0..buf.len() {
    match buf[i] {
      b'\\' => {
        w.write_all(&buf[start..i])?;
        start = i + 1;
        w.write_all(b"\\\\")?;
      },
      b'\r' => {
        w.write_all(&buf[start..i])?;
        start = i + 1;
      },
      b'\n' => {
        w.write_all(&buf[start..i])?;
        start = i + 1;
        w.write_all(b"\\n")?;
      },
      b'\t' => {
        w.write_all(&buf[start..i])?;
        start = i + 1;
        w.write_all(b"\\t")?;
      },
      c => ()
    }
  }
  if start < buf.len() {
    w.write_all(&buf[start..])?;
  }
  Ok(())
}

fn write_nl<W: Write>(w: &mut W) -> io::Result<()> {
  w.write_all(b"\n")
}

fn process_record<B: BufRead, W: Write>(rdr: &mut Reader<B>, out: &mut W, lno: &mut i32) {
  let mut buf = Vec::new();
  let mut output = false;
  let mut fno = 0;
  let mut tag = Vec::with_capacity(5);
  let mut ind1 = Vec::with_capacity(10);
  let mut ind2 = Vec::with_capacity(10);
  loop {
    match rdr.read_event(&mut buf) {
      Ok(Event::Start(ref e)) => {
        let name = str::from_utf8(e.local_name()).unwrap();
        match name {
          "record" => {
            *lno += 1
          },
          "leader" => {
            write_codes(out, *lno, fno, b"LDR", None);
            output = true;
          },
          "controlfield" => {
            fno += 1;
            let mut done = false;
            for ar in e.attributes() {
              let a = ar.expect("decode error");
              if a.key == b"tag" {
                let tag = a.unescaped_value().expect("decode error");
                write_codes(out, *lno, fno, &tag, None).expect("output error");
                done = true;
              }
            }
            assert!(done, "no tag found for control field");
            output = true;
          },
          "datafield" => {
            fno += 1;
            for ar in e.attributes() {
              let a = ar.expect("decode error");
              let v = a.unescaped_value().expect("decode error");
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
            output = true;
          },
          "subfield" => {
            fno += 1;
            let mut done = false;
            for ar in e.attributes() {
              let a = ar.expect("decode error");
              if a.key == b"code" {
                let code = a.unescaped_value().expect("decode error");
                let field = Field { ind1: &ind1, ind2: &ind2, code: &code };
                write_codes(out, *lno, fno, &tag, Some(&field)).expect("output error");
                done = true;
              }
            }
            assert!(done, "no code found for subfield");
            output = true;
          }
          _ => ()
        }
      },
      Ok(Event::End(ref e)) => {
        let name = str::from_utf8(e.local_name()).unwrap();
        match name {
          "leader" | "controlfield" | "subfield" => {
            write_nl(out).expect("output error");
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
      Ok(Event::Text(e)) => {
        if output {
          let t = e.unescaped().expect("decode error");
          write_data(out, &t).expect("output error")
        }
      },
      Ok(Event::Eof) => break,
      Err(e) => panic!("Error at position {}: {:?}", rdr.buffer_position(), e),
      _ => ()
    }
  }
}

fn main() -> io::Result<()> {
  let opt = Opt::from_args();
  let out = io::stdout();
  let mut outlock = out.lock();
  match opt.infile {
    Some(f) => {
      eprintln!("reading from compressed file {:?}", f);
      let mut fs = File::open(f)?;
      let mut gzf = GzDecoder::new(fs);
      let mut bfs = BufReader::new(gzf);
      process_delim_file(&mut bfs, &mut outlock)?;
      Ok(())
    },
    None => {
      let mut input = io::stdin();
      let mut lock = input.lock();
      process_delim_file(&mut lock, &mut outlock)?;
      Ok(())
    }
  }
}
