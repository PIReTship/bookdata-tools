//! Support structs, traits, and commands for book data commands.
use std::marker::PhantomData;
use structopt::StructOpt;
use structopt::clap::{App, ArgMatches};

use anyhow::Result;

struct CmdRef<C: Command> {
  mark: PhantomData<C>
}

/// Trait for command entries in the list.
pub trait CmdEntry {
  fn name(&self) -> String;

  fn clap<'a, 'b>(&self) -> App<'a, 'b> where 'a: 'b;

  fn run(&self, matches: &ArgMatches) -> Result<()>;
}

impl <C: Command> CmdEntry for CmdRef<C> {
  fn name(&self) -> String {
    self.clap().get_name().to_owned()
  }

  fn clap<'a, 'b>(&self) -> App<'a,'b> where 'a: 'b {
    C::clap()
  }

  fn run(&self, matches: &ArgMatches) -> Result<()> {
    let opt = C::from_clap(&matches);
    opt.exec()
  }
}

/// Trait implemented by book data commands.
pub trait Command: StructOpt + Sized + 'static {
  /// Get a command entry for this command.
  fn entry() -> Box<dyn CmdEntry> {
    let cmd: CmdRef<Self> = CmdRef {
      mark: PhantomData
    };
    Box::from(cmd)
  }

  /// Run the command with options
  fn exec(self) -> Result<()>;
}
