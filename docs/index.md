---
title: Overview
nav_order: 1
---

# Book Data Tools

The PIReT Book Data Tools are a set of tools for ingesting, integrating, and indexing
a variety of sources of book data, created by the [People and Information Research Team](https://piret.info)
at [Boise State University](https://boisestate.edu).  The result of running these tools is a PostgreSQL
database with the raw data, various useful extracted features, and integrated identifiers across the various
data sources for cross-linking.

If you use these scripts in any published research, cite [our paper][paper] ([PDF][]):

[paper]: https://md.ekstrandom.net/pubs/bag-extended
[PDF]: https://md.ekstrandom.net/pubs/bag2-preprint.pdf

> Michael D. Ekstrand and Daniel Kluver. 2021. Exploring Author Gender in Book Rating and Recommendation. <cite>User Modeling and User-Adapted Interaction</cite> (February 2021) DOI:[10.1007/s11257-020-09284-2](https://doi.org/10.1007/s11257-020-09284-2).

We also ask that you contact Michael Ekstrand to let us know about your use of the data, so we can
include your paper in our list of relying publications.

**Note:** the limitations section of the paper contains important information about
the limitations of the data these scripts compile.  **Do not use the gender information
in this data data or tools without understanding those limitations**.  In particular,
VIAF's gender information is incomplete and, in a number of cases, incorrect.

In addition, several of the data sets integrated by this project come from other sources
with their own publications.  **If you use any of the rating or interaction data, cite the
appropriate original source paper.**  For each data set below, we have provided a link to the
page that describes the data and its appropriate citation.

See the [Setup page](using/setup.md) to get started and for system requirements.

```{toctree}
:maxdepth: 1

using/index
data/index
implementation/index
papers
https://github.com/BoiseState/bookdata-tools
```

## Video

I recorded a video walking through the integration as an example for my [Data Science class](httsp://cs533.ekstrandom.net).

<iframe src="https://boisestate.hosted.panopto.com/Panopto/Pages/Embed.aspx?id=3ddd5f50-f4bf-4c27-94fb-ac4a0042ab0b&autoplay=false&offerviewer=true&showtitle=true&showbrand=false&start=0&interactivity=all" height="405" width="720" style="border: 1px solid #464646;" allowfullscreen allow="autoplay"></iframe>

## License

These tools are under the MIT license:

> Copyright 2019-2020 Boise State University
>
> Permission is hereby granted, free of charge, to any person obtaining a copy of
> this software and associated documentation files (the "Software"), to deal in
> the Software without restriction, including without limitation the rights to
> use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
> the Software, and to permit persons to whom the Software is furnished to do so,
> subject to the following conditions:
>
> The above copyright notice and this permission notice shall be included in all
> copies or substantial portions of the Software.
>
> THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
> IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
> FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
> COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
> IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
> CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
