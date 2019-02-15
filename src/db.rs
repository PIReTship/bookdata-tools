use error::Result;

use postgres::{Connection, TlsMode};

pub fn db_open(url: &Option<String>) -> Result<Connection> {
  let env = std::env::var("DB_URL");
  let url = match url {
    Some(u) => u.clone(),
    None => env?
  };

  info!("connecting to database {}", url);
  Ok(Connection::connect(url, TlsMode::None)?)
}
