---
title: Running
parent: Importing
nav_order: 4
---

# Running the Tools

The data import and integration process is scripted by [DVC](https://dvc.org).  The top-level
`dvc.yaml` pipeline depends on all required steps, so to import the data, just run:

    dvc repro

The import process will take approximately 2–3 hours.
