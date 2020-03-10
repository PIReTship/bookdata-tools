use std::io::prelude::*;

use log::*;

use anyhow::{anyhow, Result};
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
      None => Err(anyhow!("no URL provided"))
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

  /// Change the default schema
  pub fn default_schema(self, default: &str) -> DbOpts {
    DbOpts {
      db_url: self.db_url,
      db_schema: self.db_schema.or_else(|| Some(default.to_string()))
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

pub struct CopyRequest {
  db_url: String,
  schema: Option<String>,
  table: String,
  columns: Option<Vec<String>>,
  format: Option<String>,
  truncate: bool,
  name: String
}

impl CopyRequest {
  pub fn new<C: ConnectInfo>(db: &C, table: &str) -> Result<CopyRequest> {
    Ok(CopyRequest {
      db_url: db.db_url()?,
      schema: None,
      table: table.to_string(),
      columns: None,
      format: None,
      truncate: false,
      name: "copy".to_string()
    })
  }

  pub fn with_schema(self, schema: &str) -> CopyRequest {
    CopyRequest {
      schema: Some(schema.to_string()),
      ..self
    }
  }

  pub fn with_columns(self, columns: &[&str]) -> CopyRequest {
    let mut cvec = Vec::with_capacity(columns.len());
    for c in columns {
      cvec.push(c.to_string());
    }
    CopyRequest {
      columns: Some(cvec),
      ..self
    }
  }

  pub fn with_format(self, format: &str) -> CopyRequest {
    CopyRequest {
      format: Some(format.to_string()),
      ..self
    }
  }

  pub fn with_name(self, name: &str) -> CopyRequest {
    CopyRequest {
      name: name.to_string(),
      ..self
    }
  }

  pub fn truncate(self, trunc: bool) -> CopyRequest {
    CopyRequest {
      truncate: trunc,
      ..self
    }
  }

  pub fn table(&self) -> String {
    match self.schema {
      Some(ref s) => format!("{}.{}", s, self.table),
      None => self.table.clone()
    }
  }

  fn query(&self) -> String {
    let mut query = format!("COPY {}", self.table());
    if let Some(ref cs) = self.columns {
      let s = format!(" ({})", cs.join(", "));
      query.push_str(&s);
    }
    query.push_str(" FROM STDIN");
    if let Some(ref fmt) = self.format {
      query.push_str(&format!(" (FORMAT {})", fmt));
    }
    query
  }

  /// Open a writer for a copy request
  pub fn open(self) -> Result<CopyTarget> {
    let query = self.query();
    let (mut reader, writer) = pipe()?;

    let name = self.name.clone();
    let tb = thread::Builder::new().name(name.clone());
    let jh = tb.spawn(move || {
      let query = query;
      let db = connect(&self.db_url).unwrap();
      let mut cfg = postgres::transaction::Config::new();
      cfg.isolation_level(postgres::transaction::IsolationLevel::ReadUncommitted);
      let tx = db.transaction_with(&cfg).unwrap();
      if self.truncate {
        let tq = format!("TRUNCATE {}", self.table());
        info!("running {}", tq);
        tx.execute(&tq, &[]).unwrap();
      }
      info!("preparing {}", query);
      let stmt = tx.prepare(&query).unwrap();
      let n = stmt.copy_in(&[], &mut reader).unwrap();
      info!("committing copy");
      tx.commit().unwrap();
      n
    })?;
    Ok(CopyTarget {
      writer: Some(writer),
      name: name,
      thread: Some(jh)
    })
  }
}

/// Writer for copy-in operations
///
/// This writer writes to the copy-in for PostgreSQL.  It is unbuffered; you usually
/// want to wrap it in a `BufWriter`.
pub struct CopyTarget {
  writer: Option<PipeWriter>,
  name: String,
  thread: Option<thread::JoinHandle<u64>>
}

impl CopyTarget {
  fn do_close(&mut self, warn: bool) -> Result<u64> {
    if let Some(w) = self.writer.take() {
      std::mem::drop(w);
    }
    if let Some(thread) = self.thread.take() {
      match thread.join() {
        Ok(n) => {
          info!("{}: wrote {} lines", self.name, n);
          Ok(n)
        }
        Err(e) => {
          error!("{}: error: {:?}", self.name, e);
          Err(anyhow!("worker thread failed"))
        }
      }
    } else {
      if warn {
        error!("{} already shut down", self.name);
      } else {
        debug!("{} already shut down", self.name);
      }
      Ok(0)
    }
  }
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
    self.do_close(false).unwrap();
  }
}

#[test]
fn cr_initial_correct() {
  let cr = CopyRequest::new(&("foo".to_string()), "wombat").unwrap();
  assert_eq!(cr.name, "copy");
  assert_eq!(cr.db_url, "foo");
  assert_eq!(cr.table, "wombat");
  assert!(cr.columns.is_none());
  assert!(cr.schema.is_none());
  assert!(!cr.truncate);
  assert_eq!(cr.query(), "COPY wombat FROM STDIN");
}

#[test]
fn cr_set_name() {
  let cr = CopyRequest::new(&("foo".to_string()), "wombat").unwrap();
  let cr = cr.with_name("bob");
  assert_eq!(cr.name, "bob");
  assert_eq!(cr.db_url, "foo");
  assert_eq!(cr.table, "wombat");
  assert!(cr.columns.is_none());
  assert!(cr.schema.is_none());
  assert!(!cr.truncate);
}

#[test]
fn cr_set_format() {
  let cr = CopyRequest::new(&("foo".to_string()), "wombat").unwrap();
  let cr = cr.with_format("CSV");
  assert_eq!(cr.format, Some("CSV".to_string()));
  assert_eq!(cr.db_url, "foo");
  assert_eq!(cr.table, "wombat");
  assert!(cr.columns.is_none());
  assert!(cr.schema.is_none());
  assert!(!cr.truncate);
}

#[test]
fn cr_schema_propagated() {
  let cr = CopyRequest::new(&("foo".to_string()), "wombat").unwrap();
  let cr = cr.with_schema("pizza");
  assert_eq!(cr.name, "copy");
  assert_eq!(cr.db_url, "foo");
  assert_eq!(cr.table, "wombat");
  assert!(cr.columns.is_none());
  assert_eq!(cr.schema.as_ref().expect("no schema"), "pizza");
  assert!(!cr.truncate);
  assert_eq!(cr.query(), "COPY pizza.wombat FROM STDIN");
}
