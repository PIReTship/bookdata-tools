import sys
from pathlib import Path
import subprocess as sp
import os
import logging

from invoke import task, Collection
import ratings, support, viaf, openlib, loc, analyze, goodreads
from colorama import Fore as F, Back as B, Style as S

_log = logging.getLogger(__package__)

try:
    import chromalog
    chromalog.basicConfig(stream=sys.stderr, level=logging.INFO, format='%(levelname)s %(name)s: %(message)s')
except ImportError:
    logging.basicConfig(stream=sys.stderr, level=logging.INFO, format='%(levelname)s %(name)s: %(message)s')
    _log.warning('chromalog not found, using plain logs')


@task
def status(c):
    with support.database() as db, db.cursor() as cur:
        cur.execute('SELECT step, started_at, finished_at, finished_at - started_at AS elapsed FROM import_status ORDER BY started_at')
        for step, start, end, time in cur:
            if end:
                print(f'{S.BRIGHT}{step}{S.RESET_ALL}: {F.GREEN}finished{S.RESET_ALL} at {end} (took {time})')
            else:
                print(f'{S.BRIGHT}{step}{S.RESET_ALL}: {F.YELLOW}started{S.RESET_ALL} at {start}')


ns = Collection()
ns.add_task(status)
ns.add_collection(support)
ns.add_collection(ratings)
ns.add_collection(viaf)
ns.add_collection(openlib)
ns.add_collection(loc)
ns.add_collection(analyze)
ns.add_collection(goodreads)

if __name__ == '__main__':
    import invoke.program
    program = invoke.program.Program()
    program.run()
