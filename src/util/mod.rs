mod accum;
#[cfg(unix)]
pub mod process;
pub mod timing;
pub mod serde_string;

pub use accum::DataAccumulator;
pub use timing::Timer;

/// Free default function for easily constructiong defaults.
pub fn default<T: Default>() -> T {
  T::default()
}
