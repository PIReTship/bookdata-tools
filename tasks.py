import json
import os
import sys
from pathlib import Path
from subprocess import check_call

from _jsonnet import evaluate_file
from invoke.tasks import task
from yaml import safe_dump

root = Path(__file__).parent


def msg(fmt, *args, **kwargs):
    message = fmt.format(*args, **kwargs)
    print(message, file=sys.stderr)


def err(fmt, *args, **kwargs):
    message = fmt.format(*args, **kwargs)
    print("ERROR:", message, file=sys.stderr)


@task
def render_pipeline(c, dir=None, stdout=False):
    "Re-render the DVC pipeline."

    if dir:
        specs = [Path(dir) / "dvc.jsonnet"]
    else:
        specs = root.glob("**/dvc.jsonnet")

    if not specs:
        err("no spec files found")

    for path in specs:
        msg("rendering {}", path)
        results = evaluate_file(os.fspath(path))
        results = json.loads(results)
        if stdout:
            safe_dump(results, sys.stdout, width=200)
        else:
            ymlf = path.with_suffix(".yaml")
            with ymlf.open("w") as pf:
                safe_dump(results, pf, width=200)
            check_call(["dprint", "fmt", os.fspath(ymlf)])
