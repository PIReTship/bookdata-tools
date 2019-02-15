use error::Result;

use postgres::{Connection, TlsMode};
use postgres::tls::native_tls::NativeTls;

pub fn db_open(url: &Option<String>) -> Result<Connection> {
  let env = std::env::var("DB_URL");
  let url = match url {
    Some(u) => u.clone(),
    None => env?
  };

  info!("connecting to database {}", url);
  let negotiator = NativeTls::new()?;
  Ok(Connection::connect(url, TlsMode::Prefer(&negotiator))?)
}
