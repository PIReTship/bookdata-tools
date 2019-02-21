extern crate structopt;
extern crate uuid;

use structopt::StructOpt;
use uuid::Uuid;

#[derive(StructOpt, Debug)]
#[structopt(name="make-uuid")]
struct Opt {
  #[structopt(short="n", long="namespace")]
  namespace: Option<String>,
  #[structopt(name = "STRING")]
  string: Vec<String>
}

fn main() {
  let opt = Opt::from_args();

  let ns = match opt.namespace {
    None => uuid::Uuid::nil(),
    Some(ref s) if s == "url" => uuid::NAMESPACE_URL,
    Some(ref s) => uuid::Uuid::new_v5(&uuid::Uuid::nil(), s)
  };

  for s in opt.string {
    let u = uuid::Uuid::new_v5(&ns, &s);
    println!("{}\t{}", u, s);
  }
}
