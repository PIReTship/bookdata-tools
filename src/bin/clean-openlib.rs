extern crate structopt;
extern crate flate2;
extern crate bookdata;

use std::io::prelude::*;
use std::io::{self, BufReader};

use structopt::StructOpt;
use std::fs::File;
use std::path::PathBuf;
use flate2::read::GzDecoder;

use bookdata::pgutils::write_encoded;
use bookdata::tsv::split_first;

#[derive(StructOpt, Debug)]
#[structopt(name="clean-openlib")]
struct Opt {
  #[structopt(name = "FILE", parse(from_os_str))]
  infile: PathBuf
}

fn main() -> io::Result<()> {
  let opt = Opt::from_args();
  let mut fs = File::open(opt.infile)?;
  let mut gzf = GzDecoder::new(fs);
  let mut bfs = BufReader::new(gzf);
  let stdout = io::stdout();
  let mut oul = stdout.lock();
  for line in bfs.lines() {
    let ls = line?;
    let (_ty, rest) = split_first(&ls).expect("bad line");
    let (key, rest) = split_first(rest).expect("bad line");
    let (_ver, rest) = split_first(rest).expect("bad line");
    let (_stamp, json) = split_first(rest).expect("bad line");
    oul.write_all(key.as_bytes())?;
    oul.write_all(b"\t")?;
    write_encoded(&mut oul, json.as_bytes())?;
    oul.write_all(b"\n")?;
  }

  Ok(())
}
