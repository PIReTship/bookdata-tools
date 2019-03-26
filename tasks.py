import sys
from pathlib import Path
import subprocess as sp
import os
import logging

from invoke import task, Collection
import ratings, support, viaf, openlib, loc, analyze, goodreads
from colorama import Fore as F, Back as B, Style as S

_log = logging.getLogger(__package__)

_log_fmt = '%(asctime)s %(levelname)s %(name)s: %(message)s'
_log_date = '%H:%M:%S'
try:
    import chromalog
    chromalog.basicConfig(stream=sys.stderr, level=logging.INFO, format=_log_fmt, datefmt=_log_date)
except ImportError:
    logging.basicConfig(stream=sys.stderr, level=logging.INFO, format=_log_fmt, datefmt=_log_date)
    _log.warning('chromalog not found, using plain logs')


@task
def status(c):
    steps = support.get_steps(ns)
    recorded = set()
    with support.database(autocommit=True) as db, db.cursor() as cur:
        cur.execute('SELECT step, started_at, finished_at, finished_at - started_at AS elapsed FROM import_status ORDER BY started_at')
        for step, start, end, time in cur:
            recorded.add(step)
            if end:
                print(f'{S.BRIGHT}{step}{S.RESET_ALL}: {F.GREEN}finished{S.RESET_ALL} at {end} (took {time})')
            else:
                print(f'{S.BRIGHT}{step}{S.RESET_ALL}: {F.YELLOW}started{S.RESET_ALL} at {start}')

    for step in sorted(k for k in steps.keys() if k not in recorded):
        task = steps[step]
        print(f'{S.BRIGHT}{step}{S.RESET_ALL}: {F.RED}not run{S.RESET_ALL} (defined in {task})')

@task
def list_steps(c):
    steps = support.get_steps(ns)
    for s in sorted(steps.keys()):
        task = steps[s]
        print(f'{S.BRIGHT}{s}{S.RESET_ALL}: defined in {task}')


ns = Collection()
ns.add_task(status)
ns.add_task(list_steps)
ns.add_collection(support)
ns.add_collection(ratings)
ns.add_collection(viaf)
ns.add_collection(openlib)
ns.add_collection(loc)
ns.add_collection(analyze)
ns.add_collection(goodreads)

if 'DB_URL' not in os.environ and 'PGDATABASE' in os.environ:
        dbu = support.db_url()
        _log.info('initializing DB_URL=%s', dbu)
        os.environ['DB_URL'] = dbu

if __name__ == '__main__':
    import invoke.program
    program = invoke.program.Program()
    program.run()
