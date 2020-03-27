---
title: Running
parent: Usage
nav_order: 4
---

# Running the Tools

The data import and integration process is scripted by [DVC](https://dvc.org).  The top-level `Dvcfile` depends on all required steps, so to import the data, just run:

    ./dvc.sh repro

The import process will take approximately 8 hours.

## Custom DVC

Note that the command above uses `./dvc.sh` instead of calling the `dvc` executable directly.  The book
data tools customize DVC to support checking the status of database import operations, and the `./dvc.sh`
script runs DVC with the customizations installed.  If you run `dvc`, it will be unable to resolve the
`pgstat://` URLs and will fail with an error to that effect (the precise error may vary from version to
version).

`./dvc.sh` is just a wrapper and therefore takes all commands and options applicable to `dvc`.
