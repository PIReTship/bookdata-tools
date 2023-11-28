use std::fs::write;

use glob::glob;
use jrsonnet_evaluator::manifest::ManifestFormat;
use jrsonnet_evaluator::trace::PathResolver;
use jrsonnet_evaluator::{FileImportResolver, State, Val};
use jrsonnet_stdlib::{ContextInitializer, YamlFormat};

use crate::prelude::*;

/// Rerender the pipeline.
#[derive(clap::Args, Debug)]
pub struct RenderPipeline {}

fn run_pipe(path: &Path) -> Result<Val> {
    info!("rendering pipeline {}", path.display());
    let state: State = Default::default();
    state.set_import_resolver(FileImportResolver::new(vec![".".into()]));
    let ci = ContextInitializer::new(state.clone(), PathResolver::new_cwd_fallback());
    state.set_context_initializer(ci);
    match state.import(path) {
        Ok(result) => Ok(result),
        Err(e) => {
            error!("{}: evaluation failure: {}", path.display(), e.error());
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
            let result = match format.manifest(val) {
                Ok(result) => result,
                Err(e) => {
                    error!("{}: render failure: {}", path.display(), e.error());
                    for (i, level) in e.trace().0.iter().enumerate() {
                        info!("  level {}: {}", i + 1, level.desc);
                    }
                    return Err(anyhow!("jsonnet rendering failed"));
                }
            };
            let out = path.with_extension("yaml");
            info!("writing {}", out.display());
            write(&out, result.as_bytes())?;
        }
        Ok(())
    }
}
