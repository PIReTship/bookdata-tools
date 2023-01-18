use libc;
use std::mem::MaybeUninit;

use chrono::Duration;
use friendly::{bytes, duration};
use log::*;

fn timeval_duration(tv: &libc::timeval) -> Duration {
    let ds = Duration::seconds(tv.tv_sec);
    let dus = Duration::microseconds(tv.tv_usec.into());
    ds + dus
}

/// Print closing process statistics.
pub fn log_process_stats() {
    let mut usage = MaybeUninit::uninit();
    let rc = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    if rc != 0 {
        error!("getrusage failed with code {}", rc);
        return;
    }

    let usage = unsafe { usage.assume_init() };
    let user = timeval_duration(&usage.ru_utime);
    let system = timeval_duration(&usage.ru_stime);
    info!(
        "process time: {} user, {} system",
        duration(user),
        duration(system)
    );

    let mem = usage.ru_maxrss * 1024;
    info!("max RSS (memory use): {}", bytes(mem));
}
