use std::io::prelude::*;
use std::io::BufReader;
use std::path::Path;

use anyhow::Result;
use flate2::bufread::MultiGzDecoder;
use indicatif::ProgressBar;

use super::open_progress;
use crate::io::background::ThreadRead;

/// Open a gzip-compressed file for input, with a progress bar.
///
/// It sets the progress bar's prefix to the file name.
pub fn open_gzin_progress(path: &Path, pb: ProgressBar) -> Result<impl BufRead> {
    let read = open_progress(path, pb)?;
    let gzf = MultiGzDecoder::new(read);

    let thr = ThreadRead::new(gzf)?;
    let bfs = BufReader::new(thr);
    Ok(Box::new(bfs))
}
