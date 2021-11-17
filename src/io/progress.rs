use indicatif::{ProgressBar, ProgressStyle};

pub const DEFAULT_PROGRESS: &'static str = "{elapsed_precise} {bar} {percent}% {bytes}/{total_bytes} (eta: {eta})";

pub fn default_progress_style() -> ProgressStyle {
  let style = ProgressStyle::default_bar();
  let style = style.template(DEFAULT_PROGRESS);
  style
}

pub fn default_progress(n: u64) -> ProgressBar {
  let pb = ProgressBar::new(n);
  pb.set_style(default_progress_style());
  pb
}
