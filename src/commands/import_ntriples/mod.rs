mod sink;
mod idgen;

use std::io::prelude::*;
use std::io::{BufReader, BufWriter};
use std::fs::File;
use std::path::PathBuf;

use log::*;

use structopt::StructOpt;
use zip::read::{ZipArchive, ZipFile};
use indicatif::{ProgressBar, ProgressStyle};
use anyhow::{Result, anyhow};
use sha1::Sha1;
use happylog::set_progress;

use ntriple::parser::quiet_line;
use ntriple::Triple;

use crate::cleaning::{write_pgencoded};
use crate::db;
use crate::tracking::StageOpts;
use super::Command;

use sink::NodeSink;
use idgen::{IdGenerator, Target};

/// Import n-triples RDF (e.g. from LOC) into a database.
#[derive(StructOpt, Debug)]
#[structopt(name="import-ntriples")]
pub struct ImportNtriples {
  #[structopt(flatten)]
  db: db::DbOpts,

  #[structopt(flatten)]
  stage: StageOpts,

  /// Database table prefix for data
  #[structopt(short="p", long="prefix")]
  prefix: String,

  /// Truncate the database tables
  #[structopt(long="truncate")]
  truncate: bool,

  /// Input file
  #[structopt(name = "INPUT", parse(from_os_str))]
  infile: PathBuf
}

struct ImportContext {
  hash: Sha1,
  lit_out: BufWriter<db::CopyTarget>,
  triples_out: BufWriter<db::CopyTarget>,
  id_gen: IdGenerator
}

impl ImportContext {
  /// Save an RDF triple into the database.
  fn save_triple(&mut self, tr: &Triple) -> Result<()> {
    let s_id = self.id_gen.subj_id(&tr.subject)?;
    let p_id = self.id_gen.pred_id(&tr.predicate)?;
    let o_id = self.id_gen.obj_id(&tr.object)?;
    self.hash.update(s_id.as_bytes());
    self.hash.update(p_id.as_bytes());
    match o_id {
      Target::Node(oid) => {
        self.hash.update(oid.as_bytes());
        write!(&mut self.triples_out, "{}\t{}\t{}\n", s_id, p_id, oid)?;
      },
      Target::Literal(l) => {
        self.hash.update(l.data.as_bytes());
        write!(&mut self.lit_out, "{}\t{}\t", s_id, p_id)?;
        write_pgencoded(&mut self.lit_out, l.data.as_bytes())?;
        match l.data_type {
          ntriple::TypeLang::Lang(ref s) => {
            write!(&mut self.lit_out, "\t")?;
            write_pgencoded(&mut self.lit_out, s.as_bytes())?;
            write!(&mut self.lit_out, "\n")?;
          },
          _ => {
            write!(&mut self.lit_out, "\t\\N\n")?;
          }
        }
      }
    };
    Ok(())
  }
}

impl ImportNtriples {
  /// Open the input file and make sure it has 1 member
  fn open_zipfile(&self) -> Result<ZipArchive<BufReader<File>>> {
    let inf = self.infile.as_path();
    let file = File::open(inf)?;
    let file = BufReader::new(file);
    let zf = ZipArchive::new(file)?;
    if zf.len() > 1 {
      error!("{:?}: more than one member file", inf);
      return Err(anyhow!("too many input files"))
    } else if zf.len() == 0 {
      error!("{:?}: empty input archive", inf);
      return Err(anyhow!("empty input archive"));
    }
    Ok(zf)
  }

  fn open_reader<'a, R: Read + Seek>(&self, zf: &'a mut ZipArchive<R>) -> Result<ZipFile<'a>> {
    let member = zf.by_index(0)?;
    info!("processing member {:?} with {} bytes", member.name(), member.size());
    Ok(member)
  }

  /// Open a database table for copying
  fn open_table(&self, name: &str) -> Result<BufWriter<db::CopyTarget>> {
    let tbl = format!("{}_{}", self.prefix, name);
    let cpu = db::CopyRequest::new(&self.db, &tbl)?.with_name(name);
    let cpu = cpu.with_schema(self.db.schema());
    let cpu = cpu.truncate(self.truncate);
    let out = cpu.open()?;
    let out = BufWriter::new(out);
    Ok(out)
  }

  /// Set up the import context - output tables & ID generator.
  fn setup_context(&self, name: &str) -> Result<ImportContext> {
    let node_sink = NodeSink::create(&self.db);

    Ok(ImportContext {
      hash: Sha1::new(),
      lit_out: self.open_table("literals")?,
      triples_out: self.open_table("triples")?,
      id_gen: IdGenerator::create(node_sink, name)
    })
  }
}

impl Command for ImportNtriples {
  fn exec(self) -> Result<()> {
    // open input
    let mut zf = self.open_zipfile()?;
    let member = self.open_reader(&mut zf)?;

    // open database for status tracking
    let db = self.db.open()?;
    let mut stage = self.stage.begin_stage(&db)?;

    // set up output
    let mut import = self.setup_context(member.name())?;

    // set up input stream with progress and hashing
    let mut in_sf = stage.source_file(&self.infile);
    let pb = ProgressBar::new(member.size());
    pb.set_style(ProgressStyle::default_bar().template("{elapsed_precise} {bar} {percent}% {bytes}/{total_bytes} (eta: {eta})"));
    let pbr = pb.wrap_read(member);
    let pbr = in_sf.wrap_read(pbr);
    let pbr = BufReader::new(pbr);
    let _pbs = set_progress(&pb);

    // and let's start reading this stuff!
    let mut lno = 0;
    let mut nrecs = 0;
    for line in pbr.lines() {
      let line = line?;
      lno += 1;
      match quiet_line(&line) {
        Ok(Some(tr)) => {
          nrecs += 1;
          import.save_triple(&tr)?;
        },
        Ok(None) => (),
        Err(ref e) => {
          error!("error on line {}: {:?}", lno, e);
          error!("invalid line contained: {}", line);
        }
      };
    }
    pb.finish();

    // Record stage data and finish up
    let hash = in_sf.record()?;
    writeln!(stage, "READ {:?} {} {}", self.infile, nrecs, hash)?;
    let out_hash = import.hash.hexdigest();
    writeln!(stage, "WRITE {}", hash)?;
    stage.end(&Some(out_hash))?;

    Ok(())
  }
}
