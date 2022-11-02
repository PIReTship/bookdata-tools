use crate::prelude::*;
use crate::goodreads::*;
use crate::io::object::ThreadObjectWriter;
use crate::util::logging::data_progress;
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
  },
  /// Cluster GoodReads intearaction data.
  ClusterInteractions(CIOptions)
}

#[derive(StructOpt, Debug)]
pub struct ScanInput {
  /// Input file
  #[structopt(name = "INPUT", parse(from_os_str))]
  infile: PathBuf
}

/// Input options for an interaction scan
#[derive(StructOpt, Debug)]
pub struct InterInput {
  /// Book ID mapping file (only for CSV input)
  #[structopt(name="MAP", long="book-map", parse(from_os_str))]
  book_map: Option<PathBuf>,

  /// Run in CSV mode
  #[structopt(long="csv")]
  csv_mode: bool,

  #[structopt(flatten)]
  scan: ScanInput,
}

#[derive(StructOpt, Debug)]
pub struct CIOptions {
  /// Cluster ratings
  #[structopt(long="ratings")]
  ratings: bool,

  /// Cluster add-to-shelf actions
  #[structopt(long="add-actions")]
  add_actions: bool,

  /// Cluster using simple data instead of full data.
  #[structopt(long="simple")]
  simple: bool,

  /// Cluster using native GoodReads works instead of book clusters.
  #[structopt(long="native-works")]
  native_works: bool,

  /// Write output to FILE
  #[structopt(short="o", long="output", name="FILE", parse(from_os_str))]
  output: PathBuf,
}

#[derive(StructOpt, Debug)]
enum GRScan {
  /// Scan GoodReads works.
  Works(ScanInput),
  /// Scan GoodReads books.
  Books(ScanInput),
  /// Scan GoodReads genres.
  Genres(ScanInput),
  /// Scan GoodReads interactions.
  Interactions(InterInput),
}

fn scan_gr<R, W>(path: &Path, proc: W) -> Result<()>
where
  W: ObjectWriter<R> + DataSink + Send + 'static,
  R: DeserializeOwned + Send + Sync + 'static
{
  let outs: Vec<_> = proc.output_files().iter().map(|p| p.to_path_buf()).collect();

  info!("reading data from {}", path.display());
  let pb = data_progress(0);
  let read = LineProcessor::open_gzip(path, pb.clone())?;
  let mut writer = ThreadObjectWriter::new(proc);
  read.process_json(&mut writer)?;
  pb.finish_and_clear();

  writer.finish()?;

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
      },
      GRCmd::Scan { data: GRScan::Genres(opts) } => {
        info!("scanning GoodReads book genres");
        scan_gr(&opts.infile, genres::BookGenreWriter::open()?)?;
      },
      GRCmd::Scan { data: GRScan::Interactions(opts) } => {
        if opts.csv_mode {
          info!("scanning simplified GoodReads interactions");
          let books = opts.book_map.as_ref();
          let books = books.ok_or_else(|| anyhow!("book map required for CSV mode"))?;
          simple_interaction::scan_interaction_csv(books, &opts.scan.infile)?;
        } else {
          info!("scanning GoodReads interactions");
          scan_gr(&opts.scan.infile, interaction::IntWriter::open()?)?;
        }
      },
      GRCmd::ClusterInteractions(opts) => {
        let mut op = if opts.add_actions {
          cluster::ClusterOp::add_actions(&opts.output)
        } else if opts.ratings {
          cluster::ClusterOp::ratings(&opts.output)
        } else {
          error!("must specify one of --add-actions or --raitngs");
          return Err(anyhow!("no operating mode specified"));
        };
        if opts.native_works {
          op = op.native_works();
        }
        if opts.simple {
          op = op.simple();
        }

        op.cluster()?;
      },
    }

    Ok(())
  }
}
