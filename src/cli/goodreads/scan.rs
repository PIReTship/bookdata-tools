use crate::goodreads::*;
use crate::io::object::ThreadObjectWriter;
use crate::prelude::*;
use crate::util::logging::data_progress;
use serde::de::DeserializeOwned;

#[derive(clap::Subcommand, Debug)]
pub enum GRScan {
    /// Scan GoodReads works.
    Works(ScanInput),
    /// Scan GoodReads books.
    Books(ScanInput),
    /// Scan GoodReads genres.
    Genres(ScanInput),
    /// Scan GoodReads authors.
    Authors(ScanInput),
    /// Scan GoodReads interactions.
    Interactions(InterInput),
}

#[derive(Args, Debug)]
pub struct ScanInput {
    /// Input file
    #[arg(name = "INPUT")]
    infile: PathBuf,
}

/// Input options for an interaction scan
#[derive(Args, Debug)]
pub struct InterInput {
    /// Book ID mapping file (only for CSV input)
    #[arg(name = "MAP", long = "book-map")]
    book_map: Option<PathBuf>,

    /// Run in CSV mode
    #[arg(long = "csv")]
    csv_mode: bool,

    #[command(flatten)]
    scan: ScanInput,
}

fn scan_gr<R, W>(path: &Path, proc: W) -> Result<()>
where
    W: ObjectWriter<R> + DataSink + Send + Sync + 'static,
    R: DeserializeOwned + Send + Sync + 'static,
{
    let outs: Vec<_> = proc
        .output_files()
        .iter()
        .map(|p| p.to_path_buf())
        .collect();

    info!("reading data from {}", path.display());
    let pb = data_progress(0);
    let read = LineProcessor::open_gzip(path, pb.clone())?;
    let mut writer = ThreadObjectWriter::new(proc);
    read.process_json(&mut writer)?;
    pb.finish_and_clear();

    writer.finish()?;

    for out in outs {
        let outf = out.as_path();
        info!(
            "output {} is {}",
            outf.display(),
            friendly::bytes(file_size(outf)?)
        );
    }

    Ok(())
}

impl GRScan {
    pub fn exec(&self) -> Result<()> {
        match self {
            GRScan::Works(opts) => {
                info!("scanning GoodReads works");
                scan_gr(&opts.infile, work::WorkWriter::open()?)?;
            }
            GRScan::Books(opts) => {
                info!("scanning GoodReads books");
                scan_gr(&opts.infile, book::BookWriter::open()?)?;
            }
            GRScan::Genres(opts) => {
                info!("scanning GoodReads book genres");
                scan_gr(&opts.infile, genres::BookGenreWriter::open()?)?;
            }
            GRScan::Authors(opts) => {
                info!("scanning GoodReads book genres");
                scan_gr(&opts.infile, author::AuthorWriter::open()?)?;
            }
            GRScan::Interactions(opts) => {
                if opts.csv_mode {
                    info!("scanning simplified GoodReads interactions");
                    let books = opts.book_map.as_ref();
                    let books = books.ok_or_else(|| anyhow!("book map required for CSV mode"))?;
                    simple_interaction::scan_interaction_csv(books, &opts.scan.infile)?;
                } else {
                    info!("scanning GoodReads interactions");
                    scan_gr(&opts.scan.infile, interaction::IntWriter::open()?)?;
                }
            }
        };

        Ok(())
    }
}
