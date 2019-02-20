extern crate bookdata;
extern crate structopt;
extern crate indicatif;
extern crate snap;
extern crate flate2;
extern crate uuid;

use structopt::StructOpt;

use std::io::prelude::*;
use std::io::{self, BufWriter};
use std::fs::{File, OpenOptions};
use std::path::PathBuf;
use indicatif::{ProgressBar, ProgressStyle};
use uuid::Uuid;

use bookdata::tsv::split_first;

const PB_STYLE: &'static str = "{prefix}: {elapsed_precise} {bar} {percent}% {bytes}/{total_bytes} (eta: {eta})";

/// UUIDify the second lines of a node or literal TSV file
#[derive(StructOpt, Debug)]
#[structopt(name="uuidify")]
struct Opt {
  /// Decompress input files
  #[structopt(short="d", long="decompress")]
  decompress: Option<String>,

  /// Input file
  #[structopt(name = "INPUT", parse(from_os_str))]
  infile: PathBuf,

  /// Input file
  #[structopt(name = "OUTPUT", parse(from_os_str))]
  outfile: PathBuf,
}

fn main() {
  let opt = Opt::from_args();

  let inf = opt.infile.as_path();
  let fstr = inf.to_str().unwrap();
  let fs = File::open(inf).expect(&format!("{}: cannot open file", fstr));
  let pb = ProgressBar::new(fs.metadata().unwrap().len());
  pb.set_style(ProgressStyle::default_bar().template(PB_STYLE));
  pb.set_prefix(fstr);
  let pbr = pb.wrap_read(fs);
  let decomp: Box<io::Read> = match &(opt.decompress) {
    None => Box::new(pbr),
    Some(mode) => if mode == "snappy" {
      Box::new(snap::Reader::new(pbr))
    } else {
      panic!("unknown compression mode {}", mode)
    }
  };
  let buf = io::BufReader::new(decomp);

  let out = OpenOptions::new().write(true).create(true).read(false).truncate(true).open(opt.outfile);
  let out = out.unwrap();
  let out = BufWriter::new(out);
  let out = flate2::write::GzEncoder::new(out, flate2::Compression::default());
  let mut out = BufWriter::new(out);

  for line in buf.lines() {
    let line = line.unwrap();
    if let Some((_id, val)) = split_first(&line) {
      let uuid = Uuid::new_v5(&Uuid::NAMESPACE_URL, val.as_bytes());
      write!(&mut out, "{}\n", uuid).unwrap();
    }
  }
}
