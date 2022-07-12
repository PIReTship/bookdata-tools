---
title: Implementation
---

# Design and Implementation

These data and integration tools are designed to support several goals:

- Complete end-to-end reproducibility with a single command (`dvc repro`)
- Self-documenting import stage dependencies
- Automatically re-run downstream steps when a data file or integration logic changes
- Support updates (e.g. new OpenLibrary dumps) by replacing the file and re-running
- Efficient import and integration

```{toctree}
layout
dataset
```

## Implementation Principles

These goals are realized through a few technology and design decisions:

- Script all import steps with a tool that can track stage dependencies and check whether a stage is up-to-date ([DVC](https://dvc.org)).
- Make individual import stages self-contained and limited.
- Extract data from raw sources into tabular form, *then* integrate as a separate step.
- When feasible and performant, implement integration and processing steps with straightforward data join operations.
