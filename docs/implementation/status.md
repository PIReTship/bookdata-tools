---
title: Status Tracking
parent: Implementation
nav_order: 1
---

# Status Tracking
{: .no_toc}

The book tools are built around [Data Version Control](https://dvc.org), a tool for managing
data processing pipelines.  We use the software in a somewhat unusal way due to our use of
PostgreSQL as our primary storage.

Import is organized into *stages* that are also tracked in the PostgreSQL database, so that
we can get current status information from the DB about the data that has been loaded into it.

## Stages and Status

Because our primary data storage is in a database, and DVC likes to track files, our design is a
little unusual for a DVC project.  Each stage that produces output in the database (import, index,
etc.) is implemented as two stages:

- A primary stage, that produces a `.transcript` file as output; it works just like any normal
  DVC stage.
- A status stage, that depends on the `.transcript` file and produces a `.status` file.  This stage
  is marked as `always_run`, so it always re-runs even if the transcript is unchanged, so that it
  can make sure the `.status` file contains the *current* state of the database.

Stages that take database state as input depend on the corresponding `.status` file, *not* the
`.transcript` file, so that their need to update is triggered based on the current database state.
This wires together all of the dependencies, and uses the current state in the database instead of
files that might become out-of-sync with the database to track import status.
There are a couple of holes in this design, but it's the best we can do and it works.

The stage name matches the name of the `.dvc` file.

The reason for this somewhat bizarre layoutis that if we just wrote the output files, and the database
was reloaded or corrupted, the DVC status-checking logic would not be ableto keep track of it.  This
double-file design allows us to make subsequent steps depend on the actual results of the import, not
our memory of the import in the Git repository.

## In-Database Status Tracking

Import steps are tracked in the `stage_status` table in the database.  For completed stages, this can
include a key (checksum, UUID, or other identifier) to identify a 'version' of the stage.  Stages
can also have dependencies, which are solely used for computing the status of a stage (all actual
dependency relationships are handled by DVC):

- `stage_deps` tracks stage-to-stage dependencies, to say that one stage used another as input.
- `stage_file` tracks stage-to-file dependencies, to say that a stage used a file as input.

The `source_file` table tracks input file checksums.

Projects using the book database can also use `stage_status` to obtain data version information, to
see if they are up-to-date.
