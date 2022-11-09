"""
Helper to set up environments & run book data tools properly.

Most DVC stages will use this to actually run the code.  It makes
sure we compile the Rust tools, routes arguments properly, and sets
environment variables that may be needed.  For Python scripts, it
ensures the search path is set correctly.

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
from pathlib import Path
import subprocess as sp

src_dir = Path(__file__).parent
sys.path.insert(0, src_dir)


def _msg(format, *args):
    msg = format % args
    print(msg, file=sys.stderr)


def run_rust():
    # this is a rust command
    del sys.argv[1]
    # we need to fix up Rust environment in some cases
    sysroot = os.environ.get('CONDA_BUILD_SYSROOT', None)
    if sysroot and 'RUSTFLAGS' not in os.environ:
        _msg('setting Rust flags from sysroot')
        os.environ['RUSTFLAGS'] = f'-L native={sysroot}/usr/lib64 -L native={sysroot}/lib64'

    # shell out to 'cargo run' to run the command
    tool_name = sys.argv[1]

    run = ['cargo', 'run']
    if os.environ.get('BOOKDATA_DEBUG_MODE', None):
        pass  # no op
    else:
        run.append('--release')
    run.append('--')

    _msg('building and running Rust tool %s', tool_name)
    sp.run(run + sys.argv[1:], check=True)


if __name__ == '__main__':
    run_rust()
