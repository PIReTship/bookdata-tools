use std::env::var;
use std::mem::MaybeUninit;
use std::process::exit;
use std::thread::{sleep, spawn};

use anyhow::Result;
use chrono::Duration;
use friendly::{bytes, duration};
#[cfg(unix)]
use libc;
use log::*;

/// Query the number of CPUs we have.
pub fn cpu_count() -> usize {
    std::cmp::min(num_cpus::get(), num_cpus::get_physical())
}

/// Register an early-exit handler for debugging.
pub fn maybe_exit_early() -> Result<()> {
    if let Ok(v) = var("BOOKDATA_EXIT_EARLY") {
        let seconds: u64 = v.parse()?;
        info!("scheduling shutdown after {} seconds", seconds);
        spawn(move || {
            let time = std::time::Duration::from_secs(seconds);
            sleep(time);
            info!("timeout reached, exiting");
            exit(17);
        });
    }
    Ok(())
}

fn timeval_duration(tv: &libc::timeval) -> Duration {
    let ds = Duration::seconds(tv.tv_sec);
    let dus = Duration::microseconds(tv.tv_usec.into());
    ds + dus
}

#[cfg(unix)]
fn log_rusage() {
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

    let mem = usage.ru_maxrss;
    info!("max RSS (memory use): {}", bytes(mem));
}

/// Print closing process statistics.
pub fn log_process_stats() {
    #[cfg(unix)]
    log_rusage();
}
