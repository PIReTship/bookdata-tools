#[macro_use]
use log;

use error::Result;

use std::io::prelude::*;
use os_pipe::{pipe, PipeWriter};
use postgres::{Connection, TlsMode};

use std::thread;

pub fn db_open(url: &Option<String>) -> Result<Connection> {
  let env = std::env::var("DB_URL");
  let url = match url {
    Some(u) => u.clone(),
    None => env?
  };

  info!("connecting to database {}", url);
  Ok(Connection::connect(url, TlsMode::None)?)
}

pub struct CopyTarget {
  writer: Option<PipeWriter>,
  name: String,
  thread: Option<thread::JoinHandle<u64>>
}

impl Write for CopyTarget {
  fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
    self.writer.as_ref().expect("writer missing").write(buf)
  }

  fn flush(&mut self) -> std::io::Result<()> {
    self.writer.as_ref().expect("writer missing").flush()
  }
}

impl Drop for CopyTarget {
  fn drop(&mut self) {
    if let Some(w) = self.writer.take() {
      std::mem::drop(w);
    }
    if let Some(thread) = self.thread.take() {
      match thread.join() {
        Ok(n) => info!("{}: wrote {} lines", self.name, n),
        Err(e) => error!("{}: error: {:?}", self.name, e)
      };
    } else {
      error!("{} already shut down", self.name);
    }
  }
}

/// Open a writer to copy data into PostgreSQL
pub fn copy_target(url: &Option<String>, query: &str, name: &str) -> Result<CopyTarget> {
  let url = url.as_ref().map(|s| s.clone());
  let query = query.to_string();
  let (mut reader, writer) = pipe()?;
  
  let tb = thread::Builder::new().name(name.to_string());
  let jh = tb.spawn(move || {
    let query = query;
    let db = db_open(&url).unwrap();
    info!("preparing {}", query);
    let stmt = db.prepare(&query).unwrap();
    stmt.copy_in(&[], &mut reader).unwrap()
  })?;
  Ok(CopyTarget {
    writer: Some(writer),
    name: name.to_string(),
    thread: Some(jh)
  })
}
