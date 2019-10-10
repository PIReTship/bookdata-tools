import os
import sys
from pathlib import Path
import pathlib
import logging

_simple_format = logging.Formatter('{asctime} [{levelname:7s}] {name} {message}',
                                   datefmt='%Y-%m-%d %H:%M:%S',
                                   style='{')

_initialized = False

data_dir = Path('data')
tgt_dir = Path('target')
bin_dir = tgt_dir / 'release'
bdtool = bin_dir / 'bookdata'


def setup(debug=False):
    global _initialized
    ch = logging.StreamHandler(sys.stderr)
    ch.setLevel(logging.DEBUG if debug else logging.INFO)
    ch.setFormatter(_simple_format)

    root = logging.getLogger()
    root.addHandler(ch)
    root.setLevel(logging.INFO)

    logging.getLogger('dvc').setLevel(logging.ERROR)
    logging.getLogger('lenskit').setLevel(logging.DEBUG)
    logging.getLogger('').setLevel(logging.DEBUG)
    root.debug('log system configured')
    _initialized = True


def script_log(name, debug=False):
    """
    Initialize logging and get a logger for a script.

    Args:
        name(str): The ``__file__`` of the script being run.
        debug(bool): whether to enable debug logging to the console
    """

    if not _initialized:
        setup(debug)

    name = pathlib.Path(name).stem
    logger = logging.getLogger(name)

    return logger
