# Implementation

These data and integration tools are designed to support several goals:

- Complete end-to-end reproducibility with a single command (`dvc repro`)
- Self-documenting import stage dependencies
- Automatically re-run downstream steps when a data file or integration logic changes
- Support updates (e.g. new OpenLibrary dumps) by replacing the file and re-running
- Efficient import and integration

## Implementation Principles

These goals are realized through a few technology and design decisions:

- Script all import steps with a tool that can track stage dependencies and check whether a stage is up-to-date ([DVC](https://dvc.org)).
- Make individual import stages self-contained and limited.
- Extract data from raw sources into tabular form, *then* integrate as a separate step.
- When feasible and performant, implement integration and processing steps with straightforward data join operations.

## Adding or Modifying Data

1. Add the new data file(s), if necessary, to `data`, and update the
   documentation to describe how to download them.
2. Implement a `scan` stage to process the raw imported data into tabular form.
   The code can be written in either Rust or Python, depending on performance
   needs.
3. If necessary, add the inputs to the ISBN collection (under `book-links`) and clustering
   to connect it with the rest of the code.
4. Implement stages to integrate the data with the rest of the tools.  Again, this
   code can be in Rust or Python.  We usually use Polars (either the Rust or the Python
   API) to efficiently process large data files.

See the [Pipeline DSL](pipeline.md) for information about how to update the pipeline.
