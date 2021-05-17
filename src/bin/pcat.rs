use structopt::StructOpt;

use std::io::prelude::*;
use std::io;
use std::fs::File;
use std::path::{PathBuf, Path};
use indicatif::ProgressBar;
use happylog::set_progress;

use bookdata::prelude::*;
use bookdata::io::progress::default_progress_style;

#[derive(StructOpt, Debug)]
#[structopt(name="pcat")]
/// Concatenate one or more files with a progress bar.
pub struct PCat {
  #[structopt(flatten)]
  common: CommonOpts,

  /// Input file
  #[structopt(name = "FILE", parse(from_os_str))]
  infiles: Vec<PathBuf>,

  /// Input format
  #[structopt(long="format", short="f")]
  format: Option<String>
}

/// Cat a file from input to output, hashing on the way.
fn cat_file<'o, 'c, P: AsRef<Path>, W: Write>(inf: P, out: &mut W) -> Result<()> {
  let inf: &Path = inf.as_ref();
  let fstr = inf.to_string_lossy();
  info!("opening file {:?}", inf);
  let fs = File::open(inf)?;
  let pb = ProgressBar::new(fs.metadata().unwrap().len());
  pb.set_style(default_progress_style());
  pb.set_prefix(fstr.to_string());
  let _pbs = set_progress(&pb);
  let mut pbr = pb.wrap_read(fs);
  io::copy(&mut pbr, out)?;
  drop(pbr);
  Ok(())
}

fn main() -> Result<()> {
  let opts = PCat::from_args();
  opts.common.init()?;

  let stdout = io::stdout();
  let mut out = stdout.lock();

  for inf in &opts.infiles {
    cat_file(inf, &mut out)?;
  }
  Ok(())
}
