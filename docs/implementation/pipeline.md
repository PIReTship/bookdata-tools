# Pipeline Specification

[dvc]: https://dvc.org
[jsonnet]: https://jsonnet.org/
[yaml]: https://dvc.org/doc/user-guide/project-structure/dvcyaml-files

[Data Version Control][dvc] is a great tool, but its pipelines are static YAML
files with limited configurability, and substantial redundancy.  That redundancy
makes updates error-prone, and also limits our ability to do things such as
enable and disable data sets, and reconfigure which version of the [GoodReads
interaction files](/data/goodreads.qmd) we want to use.

## Generating the Pipeline {#render}

However, these YAML files are relatively easy to generate, so it's feasible to
generate them with scripts or templates.  We use [jsonnet][], a programming
language for generating JSON and similar configuration structures that
allows us to generate the pipeline with loops, conditionals, etc.  The
pipeline primary sources are in the `dvc.jsonnet` files, which we render
to produce `dvc.yaml`.

The pipelines are updated through the Rust `jrsonnet` implementation of the
jsonnet language, so it is integrated into our main executable.  You can
run this with:

```sh
./update-pipeline.sh
```

There are two exceptions to our use of jsonnet for pipelines:

-   `data/dvc.yaml` — this is maintained directly in the YAML file, because
    DVC's frozen stages don't work very well with rendering pipelines.
-   The `.dvc` files — these just record files that are added (possibly
    downloaded), we don't generate them.

The `lib.jsonnet` file provides helper routines for generating pipelines:

-   `pipeline` produces a DVC pipeline given a record of stages.
-   `cmd` takes a book data command (that would be passed to the book data
    executable) and adds the relevant bits to run it through Cargo (so
    the import software is automatically recompiled if necessary).

## Configuring the Pipeline {#config}

The pipeline can be configured through the `config.yaml` file.  We keep this
file, along with the generated pipeline, committed to git; if you change it,
we recommend working in a branch.

See the comments in that file for details.  Right now, two things can be
configured:

- Which sources of book rating and interaction data are used.
- When [GoodReads data](../data/goodreads.qmd) is enabled, which version of the
  interaction file to use.
