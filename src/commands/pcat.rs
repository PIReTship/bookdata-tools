use structopt::StructOpt;

use std::io;
use std::fs::File;
use std::path::PathBuf;
use log::*;
use indicatif::{ProgressBar, ProgressStyle};

use crate::error::Result;
use super::Command;

const PB_STYLE: &'static str = "{prefix}: {elapsed_precise} {bar} {percent}% {bytes}/{total_bytes} (eta: {eta})";

/// Concatenate one or more files with a progress bar
#[derive(StructOpt, Debug)]
#[structopt(name="pcat")]
pub struct PCat {
  /// Input file
  #[structopt(name = "FILE", parse(from_os_str))]
  infiles: Vec<PathBuf>
}

impl Command for PCat {
  fn exec(self) -> Result<()> {
    let stdout = io::stdout();
    let mut out = stdout.lock();

    for inf in self.infiles {
      let inf = inf.as_path();
      let fstr = inf.to_str().unwrap();
      info!("opening file {}", fstr);
      let fs = File::open(inf)?;
      let pb = ProgressBar::new(fs.metadata().unwrap().len());
      pb.set_style(ProgressStyle::default_bar().template(PB_STYLE));
      pb.set_prefix(fstr);
      let mut pbr = pb.wrap_read(fs);
      io::copy(&mut pbr, &mut out)?;
    }

    Ok(())
  }
}
