---
title: Data Model
has_children: true
nav_order: 3
---

# Data Model

This section describes the layout of the imported data, and the logic behind its
integration.

It doesn't describe every intermediate detail or table.

The data is organized into PostgreSQL schemas to make it easier to navigate; one effect of this is that if you just look at the default `public` schema, you will see very few of the tables.  Further, some tables are materialized views, so they may not show up in the table list.  The `\dm` command in `psql` shows materialized views.

```{toctree}
:maxdepth: 1

layout
ids
loc
openlib
viaf
bx
amazon
goodreads
cluster
gender
```
