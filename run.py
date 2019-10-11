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
setup()

if sys.argv[1] == '--rust':
    # this is a rust command
    del sys.argv[1]
    # build the Rust tools
    # TODO support alternate working directories
    _log.info('compiling Rust toolchain')
    sp.run(['cargo', 'build', '--release'], check=True)
    tool = bin_dir / 'bookdata'
    tool = os.fspath(tool)
    _log.info('running tool %s', sys.argv[1:])
    sp.run([tool] + sys.argv[1:], check=True)
else:
    script = sys.argv[1]
    _log.info('preparing to run scripts.%s', script)
    del sys.argv[1]
    runpy.run_module(f'scripts.{script}', alter_sys=True)
