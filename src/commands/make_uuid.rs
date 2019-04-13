use structopt::StructOpt;
use uuid::Uuid;

use crate::error::Result;

#[derive(StructOpt, Debug)]
#[structopt(name="make-uuid")]
pub struct Options {
  #[structopt(short="n", long="namespace")]
  namespace: Option<String>,
  #[structopt(name = "STRING")]
  string: Vec<String>
}

pub fn exec(opt: Options) -> Result<()> {
  let ns = match opt.namespace {
    None => Uuid::nil(),
    Some(ref s) if s == "url" => uuid::NAMESPACE_URL,
    Some(ref s) => uuid::Uuid::new_v5(&uuid::Uuid::nil(), s)
  };

  for s in opt.string {
    let u = Uuid::new_v5(&ns, &s);
    println!("{}\t{}", u, s);
  }

  Ok(())
}
