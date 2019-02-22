use error::{Result, err};

use std::io::prelude::*;
use os_pipe::{pipe, PipeWriter};
use postgres::{Connection, TlsMode};
use structopt::StructOpt;

use std::thread;

pub trait ConnectInfo {
  fn db_url(&self) -> Result<String>;
}

impl ConnectInfo for String {
  fn db_url(&self) -> Result<String> {
    Ok(self.clone())
  }
}

impl ConnectInfo for Option<String> {
  fn db_url(&self) -> Result<String> {
    match self {
      Some(ref s) => Ok(s.clone()),
      None => Err(err("no URL provided"))
    }
  }
}

/// Database options
#[derive(StructOpt, Debug, Clone)]
pub struct DbOpts {
  /// Database URL to connect to
  #[structopt(long="db-url")]
  db_url: Option<String>,

  /// Database schema
  #[structopt(long="db-schema")]
  db_schema: Option<String>
}

impl DbOpts {
  /// Open the database connection
  pub fn open(&self) -> Result<Connection> {
    let url = self.url()?;
    connect(&url)
  }

  pub fn url<'a>(&'a self) -> Result<String> {
    Ok(match self.db_url {
      Some(ref s) => s.clone(),
      None => std::env::var("DB_URL")?
    })
  }

  /// Get the DB schema
  pub fn schema<'a>(&'a self) -> &'a str {
    match self.db_schema {
      Some(ref s) => s,
      None => "public"
    }
  }
}

impl ConnectInfo for DbOpts {
  fn db_url(&self) -> Result<String> {
    self.url()
  }
}

pub fn connect(url: &str) -> Result<Connection> {
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
pub fn copy_target<C: ConnectInfo>(ci: &C, query: &str, name: &str) -> Result<CopyTarget> {
  let url = ci.db_url()?;
  let query = query.to_string();
  let (mut reader, writer) = pipe()?;
  
  let tb = thread::Builder::new().name(name.to_string());
  let jh = tb.spawn(move || {
    let query = query;
    let db = connect(&url).unwrap();
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

/// Truncate a table
pub fn truncate_table<C: ConnectInfo>(ci: &C, table: &str, schema: &str) -> Result<()> {
  let url = ci.db_url()?;
  let db = connect(&url)?;
  let q = format!("TRUNCATE {}.{}", schema, table);
  info!("running {}", q);
  db.execute(&q, &[])?;
  Ok(())
}
