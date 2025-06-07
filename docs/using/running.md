# Running the Tools

The data import and integration process is scripted by [DVC](https://dvc.org)
and is fully-automated.  After you have downloaded the [source data](./sources.md),
you can run the integration:

```console
$ pixi run repro-all
```

:::{.callout-note}
You do not need to have the Pixi environment active to run the above command.
:::

Internally, this runs the DVC `repro` command inside the Pixi environment:

```console
$ dvc repro --all-pipelines
```

You can also directly run `dvc repro` commands to run parts of the pipeline.

If you have [configured a remote](storage.md) to store your data files, you can
then run `dvc push` to push the files to the remote to share with others on your
team, copy to another computer, or import into another project.
