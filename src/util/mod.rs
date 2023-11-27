//! Various utility modules.
mod accum;
pub mod logging;
#[cfg(unix)]
pub mod process;
pub mod serde_string;
pub mod timing;
pub mod unicode;

pub use accum::StringAccumulator;
pub use timing::Timer;

/// Free default function for easily constructiong defaults.
pub fn default<T: Default>() -> T {
    T::default()
}
