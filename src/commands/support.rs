use structopt::StructOpt;
use structopt::clap::{App, ArgMatches};

use crate::error::Result;

pub struct CmdEntry<'a> {
  app: App<'a,'a>,
  runner: fn(&ArgMatches) -> Result<()>
}

impl <'a> CmdEntry<'a> {
  pub fn name(&self) -> &str {
    self.app.get_name()
  }

  pub fn app(&self) -> &App<'a,'a> {
    &self.app
  }

  pub fn run(&self, matches: &ArgMatches) -> Result<()> {
    (self.runner)(&matches)
  }
}

pub trait Command: StructOpt + Sized {
  /// Run the command with options
  fn exec(self) -> Result<()>;

  /// Run the command from arg matches
  fn exec_from_clap(matches: &ArgMatches) -> Result<()> {
    let opt = Self::from_clap(&matches);
    opt.exec()
  }

  /// Get a command entry
  fn get_entry<'a>() -> CmdEntry<'a> {
    CmdEntry {
      app: Self::clap(),
      runner: Self::exec_from_clap
    }
  }
}
