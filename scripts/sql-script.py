"""
Usage:
    sql-script.py [options] SCRIPT

Options:
    -T, --transcript FILE
        Write the execution transcript to FILE.
    -s, --stage-name NAME
        Record as stage NAME.
    --dry-run
        Print the script's information without actually running it.
    --verbose
        Verbose logging information.
    SCRIPT
        The script to run.
"""

import os
import sys
import re
import time
from pathlib import Path
from datetime import timedelta
from typing import NamedTuple, List
from docopt import docopt

import psycopg2
from more_itertools import peekable
import sqlparse

from bookdata import script_log
from bookdata import db

opts = docopt(__doc__)
_log = script_log(__name__, opts.get('--verbose'))

script_file = Path(opts.get('SCRIPT'))

tfile = opts.get('-T', None)
if tfile:
    tfile = Path(tfile)
else:
    tfile = script_file.with_suffix('.transcript')

stage = opts.get('-s', None)
if not stage:
    stage = script_file.stem

_log.info('reading %s', script_file)
script = db.SqlScript(script_file)
_log.info('%s has %d chunks', script_file, len(script.chunks))
if opts.get('--dry-run'):
    script.describe()
else:
    with tfile.open('w') as txf, db.connect() as dbc:
        with dbc, dbc.cursor() as cur:
            db.begin_stage(cur, stage)
            db.hash_and_record_file(cur, script_file, stage)
        script.execute(dbc, transcript=txf)
        with dbc, dbc.cursor() as cur:
            db.end_stage(cur, stage)
