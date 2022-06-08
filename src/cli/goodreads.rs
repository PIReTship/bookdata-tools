use std::mem::drop;
use crate::prelude::*;
use crate::goodreads::*;
use crate::io::object::ThreadWriter;
use crate::util::logging::data_progress;
use crate::util::logging::set_progress;
use serde::de::DeserializeOwned;

/// GoodReads processing commands.
#[derive(StructOpt, Debug)]
pub struct Goodreads {
  #[structopt(subcommand)]
  command: GRCmd
}

#[derive(StructOpt, Debug)]
enum GRCmd {
  /// Scan GoodReads data.
  Scan {
    #[structopt(subcommand)]
    data: GRScan
  }
}

#[derive(StructOpt, Debug)]
pub struct ScanInput {
  /// Input file
  #[structopt(name = "INPUT", parse(from_os_str))]
  infile: PathBuf
}

#[derive(StructOpt, Debug)]
enum GRScan {
  /// Scan GoodReads works.
  Works(ScanInput),
  /// Scan GoodReads books.
  Books(ScanInput),
  /// Scan GoodReads interactions.
  Interactions(ScanInput),
}

fn scan_gr<R, W>(path: &Path, proc: W) -> Result<()>
where
  W: ObjectWriter<R> + DataSink + Send + 'static,
  R: DeserializeOwned + Send + Sync + 'static
{
  let path: &Path = path.as_ref();
  let outs: Vec<_> = proc.output_files().iter().map(|p| p.to_path_buf()).collect();

  info!("reading data from {}", path.display());
  let pb = data_progress(0);
  let read = LineProcessor::open_gzip(path, pb.clone())?;
  let mut writer = ThreadWriter::new(proc);
  let _lg = set_progress(pb);
  read.process_json(&mut writer)?;

  writer.finish()?;
  drop(_lg);

  for out in outs {
    let outf = out.as_path();
    info!("output {} is {}", outf.display(), friendly::bytes(file_size(outf)?));
  }

  Ok(())
}

impl Command for Goodreads {
  fn exec(&self) -> Result<()> {
    match &self.command {
      GRCmd::Scan { data: GRScan::Works(opts) } => {
        info!("scanning GoodReads works");
        scan_gr(&opts.infile, work::WorkWriter::open()?)?;
      }
      GRCmd::Scan { data: GRScan::Books(opts) } => {
        info!("scanning GoodReads books");
        scan_gr(&opts.infile, book::BookWriter::open()?)?;
      }
      GRCmd::Scan { data: GRScan::Interactions(opts) } => {
        info!("scanning GoodReads interactions");
        scan_gr(&opts.infile, interaction::IntWriter::open()?)?;
      }
    }

    Ok(())
  }
}
