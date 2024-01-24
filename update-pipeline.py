#!/usr/bin/env python3
"""
Re-render the pipeline from current configuration and definition files.

Usage:
    update-pipeline.py [-s] [DIR...]

Options:
    DIR         pipeline directory to render
    -s          render to stdout instead of file
"""

import os
import sys
from pathlib import Path
import json

from docopt import docopt
from _jsonnet import evaluate_file
from yaml import safe_dump


def msg(fmt, *args, **kwargs):
    message = fmt.format(*args, **kwargs)
    print(message, file=sys.stderr)


def err(fmt, *args, **kwargs):
    message = fmt.format(*args, **kwargs)
    print("ERROR:", message, file=sys.stderr)


def render_pipeline(path: Path, stdout=False):
    msg("rendering {}", path)
    results = evaluate_file(os.fspath(path))
    results = json.loads(results)
    if stdout:
        safe_dump(results, sys.stdout, width=200)
    else:
        with path.with_suffix(".yaml").open("w") as pf:
            safe_dump(results, pf, width=200)


def main(opts):
    dirs = opts["DIR"]
    if dirs:
        specs = (Path(p) / "dvc.jsonnet" for p in dirs)
    else:
        root = Path(".")
        if not (root / "update-pipeline.py").exists():
            raise RuntimeError("must be run from repo root")
        specs = root.glob("**/dvc.jsonnet")

    if not specs:
        err("no spec files found")

    for specfile in specs:
        render_pipeline(specfile, opts["-s"])


if __name__ == "__main__":
    options = docopt(__doc__)
    main(options)
