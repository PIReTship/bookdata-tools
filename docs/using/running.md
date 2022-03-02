---
title: Running
---

# Running the Tools

The data import and integration process is scripted by [DVC](https://dvc.org).  The top-level
`dvc.yaml` pipeline depends on all required steps for the the core data, so to import the data,
just run:

    dvc repro

The import process will take approximately 2–3 hours on a reasonably fast computer.

There are some additional useful outputs that the main pipeline does not invoke; you can generate
these with:

    dvc repro --all-pipelines

If you have [configured a remote](./remote.md) to store your data files, you can
then run `dvc push` to push the files to the remote to share with others on your
team, copy to another computer, or import into another project.
