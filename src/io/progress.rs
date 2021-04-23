use indicatif::ProgressStyle;

pub const DEFAULT_PROGRESS: &'static str = "{elapsed_precise} {bar} {percent}% {bytes}/{total_bytes} (eta: {eta})";

pub fn default_progress_style() -> ProgressStyle {
  let style = ProgressStyle::default_bar();
  let style = style.template(DEFAULT_PROGRESS);
  style
}
