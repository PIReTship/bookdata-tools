This repository contains the code to import and integrate the book and rating data that we work
with. It imports and integrates data from several sources in a homogenous tabular outputs; import
scripts are primarily Rust, with Python to orchestrate things and and implement analyses, and
DataFusion SQL for some integration steps.

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

See the [documentation site](https://bookdata.piret.info) for details on using and extending
these tools.

## Running Everything

The dependencies are declared in `pyproject.toml`, in a format suitable for use with
[conda-lock][].  We provide lockfiles for Windows, macOS, and Linux; to create a Conda
environment, run:

    conda create -n bookdata --file conda-linux-64.lock

You can run the entire import process with:

    dvc repro

To regenerate or update the lockfiles, run:

    conda-lock lock --mamba -f pyproject.toml

[conda-lock]: https://github.com/conda-incubator/conda-lock/

## Copyright and Acknowledgements

Copyright &copy; 2020 Boise State University.  Distributed under the MIT License; see LICENSE.md.
This material is based upon work supported by the National Science Foundation under
Grant No. IIS 17-51278. Any opinions, findings, and conclusions or recommendations
expressed in this material are those of the author(s) and do not necessarily reflect
the views of the National Science Foundation.
