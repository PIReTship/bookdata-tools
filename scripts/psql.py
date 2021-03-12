"""
Spawn a PSQL shell with configuration from the DB configuration.

Usage:
    psql.py [args]
"""

import os
import sys
import re
import time
import hashlib
import subprocess as sp
from pathlib import Path
from datetime import timedelta
from typing import NamedTuple, List

from bookdata import script_log
from bookdata import db, tracking

_log = script_log(__name__)

config = db.DBConfig.load()
os.environ['PGHOST'] = config.host
os.environ['PGDATABASE'] = config.database
if config.port:
    os.environ['PGPORT'] = config.port
if config.user:
    os.environ['PGUSER'] = config.user
if config.password:
    os.environ['PGPASSWORD'] = config.password

_log.info('spawning psql')
sp.run(['psql'] + sys.argv[1:], check=True)
