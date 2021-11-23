"""
Helper to set up environments & run book data tools properly.

Usage:
    run.py --rust TOOL ARGS...
    run.py SCRIPT ARGS...

Options:
    --rust
        Run a Rust tool instead of a Python script.
    TOOL
        The name of the Rust tool to run
    SCRIPT
        The name of the Python script to run.
    ARGS
        The arguments to the tool or script.
"""

import os
import os.path
import sys
import runpy
from pathlib import Path
import logging
import subprocess as sp

_log = logging.getLogger('run.py')

src_dir = Path(__file__).parent
sys.path.insert(0, src_dir)

from bookdata import setup, bin_dir


def run_rust():
    # this is a rust command
    del sys.argv[1]
    # we need to fix up Rust environment in some cases
    sysroot = os.environ.get('CONDA_BUILD_SYSROOT', None)
    if sysroot and 'RUSTFLAGS' not in os.environ:
        _log.info('setting Rust flags from sysroot')
        os.environ['RUSTFLAGS'] = f'-L native={sysroot}/usr/lib64 -L native={sysroot}/lib64'

    # shell out to 'cargo run' to run the command
    tool_name = sys.argv[1]

    _log.info('building and running Rust tool %s', tool_name)
    sp.run(['cargo', 'run', '--release', '--'] + sys.argv[1:], check=True)


def run_script():
    script = sys.argv[1]
    del sys.argv[1]
    if os.path.exists(script):
        _log.info('running %s', script)
        runpy.run_path(script)
    else:
        _log.info('preparing to run scripts.%s', script)
        runpy.run_module(f'scripts.{script}', alter_sys=True)


if __name__ == '__main__':
    setup()
    if sys.argv[1] == '--rust':
        run_rust()
    else:
        run_script()
