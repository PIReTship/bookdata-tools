use structopt::StructOpt;
use uuid::Uuid;
use anyhow::Result;

use super::Command;

#[derive(StructOpt, Debug)]
#[structopt(name="make-uuid")]
pub struct MakeUuid {
  #[structopt(short="n", long="namespace")]
  namespace: Option<String>,
  #[structopt(name = "STRING")]
  string: Vec<String>
}

impl Command for MakeUuid {
  fn exec(self) -> Result<()> {
    let ns = match self.namespace {
      None => Uuid::nil(),
      Some(ref s) if s == "url" => uuid::NAMESPACE_URL,
      Some(ref s) => uuid::Uuid::new_v5(&uuid::Uuid::nil(), s)
    };

    for s in self.string {
      let u = Uuid::new_v5(&ns, &s);
      println!("{}\t{}", u, s);
    }

    Ok(())
  }
}
