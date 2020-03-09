"""
Usage:
    stage-status.py [options] STAGE

Options:
    --timestamps
        Include timestamps in stage status.
    -o FILE
        Write output to FILE.
    STAGE
        The stage to check.
"""

import sys
from docopt import docopt
from bookdata import tracking, script_log

_log = script_log(__name__)
opts = docopt(__doc__)

timestamps = opts.get('--timestamps')

stage = opts.get('STAGE')
out = opts.get('-o', None)

if out is None or out == '-':
    sf = sys.stdout
else:
    sf = open(out, 'w')


tracking.stage_status(stage, sf, timestamps=timestamps)
