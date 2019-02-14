use std::error::Error;
use std::sync::mpsc::channel;

use streaming_iterator::StreamingIterator;

use postgres::{Connection, TlsMode, Type};
use postgres::types::ToSql;
use postgres::tls::native_tls::NativeTls;

pub fn db_open(url: &Option<String>) -> Result<Connection, Box<Error>> {
  let env = std::env::var("DB_URL");
  let url = match url {
    Some(u) => u.clone(),
    None => env?
  };

  info!("connecting to database {}", url);
  let negotiator = NativeTls::new()?;
  Ok(Connection::connect(url, TlsMode::Prefer(&negotiator))?)
}

enum CopyMsg {
  Init,
  Row(Vec<Box<ToSql>>),
  Close
}

pub struct CopyTarget {
  sender: std::sync::mpsc::Sender<CopyMsg>,
  thread: std::thread::Thread
}

pub struct CopyWorker {
  recv: std::sync::mpsc::Receiver<CopyMsg>,
  db: Connection,
  table: String,
  val: CopyMsg,
  pos: i32
}

impl CopyWorker {
  fn pump(&mut self) {
    let query = format!("COPY {} FROM STDIN (FORMAT binary)", self.table);
    let stmt = self.db.prepare(query).unwrap();

  }
}

impl StreamingIter for CopyWorker {
  fn advance(&mut self) {
    let adv = match self.val {
      Init => true,
      Row(v) => pos >= v.len(),
      Close => false
    };
    if adv {
      self.val = self.recv.recv().unwrap();
      self.pos = 0;
    } else {
      self.pos += 1;
    }
  }

  fn get(&self) -> Option<&Box<ToSql>> {
    match self.val {
      Row(v) => Some(&v[self.pos]),
      _ => None
    }
  }
}

impl CopyTarget {
  fn connect(url: &Option<String>, table: &str, types: &[Type]) -> Result<Connection, Box<Error>> {
    let db = db_open(&url);
    let (tx, rx) = channel();
    let thread = spawn(|| {
    });
  }
}
