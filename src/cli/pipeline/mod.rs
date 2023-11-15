use std::fs::{read_to_string, write};

use glob::glob;
use jrsonnet_evaluator::manifest::ManifestFormat;
use jrsonnet_evaluator::parser::{parse, ParserSettings, Source, SourceFile, SourcePath};
use jrsonnet_evaluator::{evaluate, ContextBuilder, FileImportResolver, State, Val};
use jrsonnet_stdlib::YamlFormat;

use crate::prelude::*;

/// Rerender the pipeline.
#[derive(clap::Args, Debug)]
pub struct RenderPipeline {}

fn run_pipe(path: &Path) -> Result<Val> {
    info!("rendering pipeline {}", path.display());
    let sp = SourceFile::new(path.to_owned());
    let text = read_to_string(path)?;
    let source = Source::new(SourcePath::new(sp), (&text).into());
    let script = parse(&text, &ParserSettings { source })?;
    let state: State = Default::default();
    state.set_import_resolver(FileImportResolver::new(vec![".".into()]));
    let context = ContextBuilder::new(state).build();
    match evaluate(context, &script) {
        Ok(result) => Ok(result),
        Err(e) => {
            error!("{}: failed to run jsonnet: {}", path.display(), e.error());
            Err(anyhow!("jsonnet evaluation failed"))
        }
    }
}

impl Command for RenderPipeline {
    fn exec(&self) -> Result<()> {
        let paths = glob("**/dvc.jsonnet")?;
        for path in paths {
            let path = path?;
            let val = run_pipe(&path)?;
            let format = YamlFormat::cli(2, true);
            let result = format.manifest(val).expect("yaml render failure");
            let out = path.with_extension("yaml");
            info!("writing {}", out.display());
            write(&out, result.as_bytes())?;
        }
        Ok(())
    }
}
