"""
Run a Python script.  The script name should come from a script name in 'scripts'.
"""

import os
import sys
import runpy
from pathlib import Path
import logging
import subprocess as sp

_log = logging.getLogger('run.py')

src_dir = Path(__file__).parent
sys.path.insert(0, src_dir)

from bookdata import setup, bin_dir
from bookdata.db import db_url


def run_rust():
    # this is a rust command
    del sys.argv[1]
    # we need to fix up Rust environment
    sysroot = os.environ.get('CONDA_BUILD_SYSROOT', None)
    if sysroot and 'RUSTFLAGS' not in os.environ:
        _log.info('setting Rust flags from sysroot')
        os.environ['RUSTFLAGS'] = f'-L native={sysroot}/usr/lib64 -L native={sysroot}/lib64'
    # build the Rust tools
    # TODO support alternate working directories
    _log.info('compiling Rust tools')
    sp.run(['cargo', 'build', '--release'], check=True)

    tool_name = sys.argv[1]
    tool = bin_dir / tool_name
    args = sys.argv[2:]
    if sys.platform == 'win32':
        tool = tool.with_suffix('.exe')
    if tool.exists():
        _log.info('running program %s', tool_name)
    else:
        tool = bin_dir / 'bookdata'
        args = sys.argv[1:]
        _log.info('running tool %s', tool_name)

    tool = os.fspath(tool)

    if 'DB_URL' not in os.environ:
        os.environ['DB_URL'] = db_url()
    sp.run([tool] + args, check=True)


def run_script():
    script = sys.argv[1]
    _log.info('preparing to run scripts.%s', script)
    del sys.argv[1]
    runpy.run_module(f'scripts.{script}', alter_sys=True)


if __name__ == '__main__':
    setup()
    if sys.argv[1] == '--rust':
        run_rust()
    else:
        run_script()
