"""
Output configuration information.

Usage:
    config.py --database (--url | --env)
"""

from docopt import docopt

from bookdata import db
from bookdata import script_log

_log = script_log(__file__)


def _print_env(src, attr, var):
    val = getattr(src, attr, None)
    if val is not None:
        print(f"export {var}='{val}'")


def db_config(opts):
    cfg = db.DBConfig.load()
    if opts['--url']:
        print(cfg.url())
    elif opts['--env']:
        _print_env(cfg, 'host', 'PGHOST')
        _print_env(cfg, 'port', 'PGPORT')
        _print_env(cfg, 'database', 'PGDATABASE')
        _print_env(cfg, 'user', 'PGUSER')
        _print_env(cfg, 'password', 'PGPASSWORD')


if __name__ == '__main__':
    opts = docopt(__doc__)
    if opts['--database']:
        db_config(opts)
