---
title: Pipeline DSL
---


# Pipeline DSL

[dvc]: https://dvc.org
[plumber]: https://github.com/mdekstrand/plumber-dsl
[yaml]: https://dvc.org/doc/user-guide/project-structure/dvcyaml-files

[Data Version Control][dvc] is a great tool, but its pipelines are static YAML
files with limited configurability, and substantial redundancy.  However, these
YAML files are relatively easy to generate, so it's feasible to generate them
with scripts or templates.

To that end, we have implemented a small DSL (domain-specific language) for
emitting DVC pipelines.  This language is built in TCL, so you have full
programming capabilities, but you don't need to know TCL to add or edit stages.
The DSL code lives in a [separate repository][plumber], included here as a Git
submodule.

Therefore, to edit the pipeline, don't edit the `dvc.yaml` files directly —
instead, edit the corresponding `pipeline.tcl` file, and re-render with:

    tclsh plumber/render.tcl

There are two exceptions to this:

-   `data/dvc.yaml` — this is maintained directly in the YAML file, because
    DVC's frozen stages don't work very well with rendering pipelines.
-   The `.dvc` files — these just record files that are added (possibly
    downloaded), we don't generate them.

## Pipeline DSL

The DSL provides two primary top-level commands:

`stage`
:   The `stage` command defines a stage.  It is used as follows:

    ```tcl
    stage my-stage-name {
        cmd process-data.py
        dep input.parquet
        out output.parquet
    }
    ```

    It is also possible to generate DVC foreach stages, but these are not used
    very much in our code.

`subdir`
:   The `subdir` command adds a subdirectory.  The command:

    ```tcl
    subdir foo
    ```

    renders `foo/pipeline.tcl` to create `foo/dvc.yaml`.

## Stage DSL

Within a `stage` block, the following commands are available (they are sugar for
the underlying DVC pipeline YAML entries — see [the docs][yaml] for details):

`cmd`
:   The command to run for this stage. Arguments are joined with spaces and passed
    as-is to the underlying YAML.  Since TCL interprets characters such as quotes and
    `$`, you can use brace-quoting to preserve special characters for the shell:

        cmd my-stage-command {"quoted-argument"}

    will produce:

        my-stage-command "quoted-argument"

`wdir`
:   Specify the working directory for the stage.  All paths, including deps and outs,
    are relative to this directory.

`dep`
:   Specify a dependency for the stage:

    ```tcl
    dep input-file.parquet
    ```

    Specify multiple times for multiple dependencies.  You can also specify multiple
    dependencies on a single line as separate arguments.  If an argument has spaces
    (very much not recommended), just use double quotes like in the shell.

`out`
:   Specify an output for the stage:

    ```tcl
    out output-file.parquet
    ```

    This option takes a couple of options.  `-nocache` turns off the DVC cache for
    the output, for small files that you want tracked directly in Git instead of
    separately in the DVC cache:

    ```tcl
    out -nocache some-summary-output.json
    ```

    The `-metric` option registers the output as a metric instead of an output, so
    it works with the DVC metric commands.

Within this project's pipeline, we define an additional command `bdcmd`, that
emits a `cmd` that runs one of the Rust-implemented book data commands.  With
this we don't need to specify the Cargo build options or the `bookdata`
executable for each stage that uses it; we can just use `bdcmd` to abbreviate
all of that.
