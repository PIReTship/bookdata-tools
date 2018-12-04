extern crate structopt;
extern crate flate2;
extern crate indicatif;
extern crate bookdata;

use std::io::prelude::*;
use std::io::{self, BufReader};

use structopt::StructOpt;
use std::fs::File;
use std::path::PathBuf;
use flate2::read::GzDecoder;
use indicatif::{ProgressBar, ProgressStyle};

use bookdata::pgutils::write_encoded;
use bookdata::tsv::split_first;

#[derive(StructOpt, Debug)]
#[structopt(name="clean-openlib")]
struct Opt {
  #[structopt(name = "INPUT", parse(from_os_str))]
  infile: PathBuf
}

fn process<R: BufRead, W: Write>(src: &mut R, dst: &mut W) -> io::Result<()> {
  for line in src.lines() {
    let ls = line?;
    let (_ty, rest) = split_first(&ls).expect("bad line");
    let (key, rest) = split_first(rest).expect("bad line");
    let (_ver, rest) = split_first(rest).expect("bad line");
    let (_stamp, json) = split_first(rest).expect("bad line");
    dst.write_all(key.as_bytes())?;
    dst.write_all(b"\t")?;
    write_encoded(dst, json.as_bytes())?;
    dst.write_all(b"\n")?;
  }

  Ok(())
}

fn main() -> io::Result<()> {
  let opt = Opt::from_args();
  let stdout = io::stdout();
  let mut out = stdout.lock();

  let mut fs = File::open(opt.infile)?;
  let pb = ProgressBar::new(fs.metadata()?.len());
  pb.set_style(ProgressStyle::default_bar().template("{elapsed_precise} {percent:.bold} {bar} {bytes}/{total_bytes} (eta: {eta})"));

  let mut pbr = pb.wrap_read(fs);
  let mut gzf = GzDecoder::new(pbr);
  let mut bfs = BufReader::new(gzf);

  process(&mut bfs, &mut out)
}
