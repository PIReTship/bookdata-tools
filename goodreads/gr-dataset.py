"""
Create a LensKit dataset for GoodReads recommendation data.

Usage:
    gr-dataset.py [options] --works DIR

Options:
    -v, --verbose   enable verbose log output
    --works         create a summarized work-level dataset
    --core          use the 5-core data instead of full
    DIR             write dataset to DIR
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import polars as pl
from docopt import docopt
from lenskit.data import DatasetBuilder
from lenskit.logging import LoggingConfig, get_logger

_log = get_logger("gr-dataset")

GR_WORK_BASE = 400_000_000
GR_BOOK_BASE = 500_000_000


def main():
    opts = docopt(__doc__ or "")
    lc = LoggingConfig()
    if opts["--verbose"]:
        lc.set_verbose()
    lc.apply()

    out_dir = Path(opts["DIR"])

    if opts["--works"]:
        data = work_dataset(opts)
    else:
        _log.error("no mode specified")
        sys.exit(2)

    _log.info("saving dataset")
    data.save(out_dir)


def work_dataset(opts) -> DatasetBuilder:
    log = _log.bind(items="works")
    bld = DatasetBuilder("gr-works")

    log.debug("loading actions")
    if opts["--core"]:
        actions = pd.read_parquet("gr-work-actions-5core.parquet")
    else:
        actions = pd.read_parquet("gr-work-actions.parquet")
    log.info("loaded actions", action_count=len(actions))
    actions = actions.rename(
        columns={
            "last_time": "timestamp",
            "last_rating": "rating",
        }
    )
    actions["timestamp"] = pd.to_datetime(actions["timestamp"], unit="s")
    actions["last_time"] = pd.to_datetime(actions["last_time"], unit="s")
    bld.add_interactions(
        "shelve", actions, entities=["user", "item"], missing="insert", default=True
    )


if __name__ == "__main__":
    main()
