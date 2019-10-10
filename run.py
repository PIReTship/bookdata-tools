"""
Run a Python script.  The script name should come from a script name in 'scripts'.
"""

import sys
import runpy
from pathlib import Path
import logging

_log = logging.getLogger('run.py')

src_dir = Path(__file__).parent
sys.path.insert(0, src_dir)

from bookdata import setup
setup()

script = sys.argv[1]
_log.info('preparing to run %s', script)
del sys.argv[1]

runpy.run_module(f'scripts.{script}', alter_sys=True)
