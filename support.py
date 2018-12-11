import sys
import os
from pathlib import Path
import subprocess as sp
from invoke import task
import psycopg2
import logging

_log = logging.getLogger(__name__)

data_dir = Path('data')
tgt_dir = Path('target')
bin_dir = tgt_dir / 'release'
numspaces = dict(work=100000000, edition=200000000, rec=300000000,
                 gr_work=400000000, gr_book=500000000,
                 isbn=900000000)

def db_url():
    if 'DB_URL' in os.environ:
        return os.environ['DB_URL']
    
    host = os.environ.get('PGHOST', 'localhost')
    port = os.environ.get('PGPORT', None)
    db = os.environ['PGDATABASE']
    user = os.environ.get('PGUSER', None)
    pw = os.environ.get('PGPASSWORD', None)

    url = 'postgresql://'
    if user:
        url += user
        if pw:
            url += ':' + pw
        url += '@'
    url += host
    if port:
        url += ':' + port
    url += '/' + db
    return url


@task
def init(c):
    "Make sure initial database structure are in place"
    if start('init', fail=False):
        c.run("psql -f common-schema.sql")
        finish('init')


@task
def build(c, debug=False):
    "Compile the Rust support executables"
    global bin_dir
    if debug:
        _log.info('compiling support executables in debug mode')
        c.run('cargo build')
        bin_dir = tgt_dir / 'debug'
    else:
        _log.info('compiling support executables')
        c.run('cargo build --release')


def pipeline(steps, outfile=None):
    last = sp.DEVNULL
    if outfile is not None:
        outfd = os.open(outfile, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o666)
    else:
        outfd = None

    procs = []
    for step in steps[:-1]:
        _log.debug('running %s', step)
        proc = sp.Popen(step, stdin=last, stdout=sp.PIPE)
        last = proc.stdout
        procs.append(proc)

    proc = sp.Popen(steps[-1], stdin=last, stdout=outfd)
    procs.append(proc)

    for p, s in zip(procs, steps):
        rc = p.wait()
        if rc != 0:
            _log.error(f'{s[0]} exited with code {rc}')
            raise RuntimeError('subprocess failed')


class database:
    def __init__(self, autocommit = False, dbc=None):
        self.autocommit = autocommit
        self.connection = dbc
        self.need_close = False

    def __enter__(self):
        if self.connection is None:
            _log.debug('connecting to database')
            self.connection = psycopg2.connect("")
            self.need_close = True

            if self.autocommit:
                self.connection.set_session(autocommit=True)

        return self.connection

    def __exit__(self, *args):
        if self.need_close:
            _log.debug('closing DB connection')
            self.connection.close()
            self.need_close = False


def check_prereq(step, dbc=None):
    _log.debug('checking prereq %s', step)
    with database(dbc=dbc, autocommit=True) as db:
        with db.cursor() as cur:
            cur.execute('''
                SELECT finished_at FROM import_status 
                WHERE step = %s AND finished_at IS NOT NULL
            ''', [step])
            res = cur.fetchone()
            if not res:
                _log.error('prerequisite step %s not completed', step)
                raise RuntimeError('prerequisites not met')


def start(step, force=False, fail=True, dbc=None):
    _log.debug('starting step %s', step)
    with database(dbc=dbc, autocommit=True) as db:
        with db.cursor() as cur:
            cur.execute('''
                SELECT finished_at FROM import_status 
                WHERE step = %s
            ''', [step])
            res = cur.fetchone()
            if res:
                date, = res
                if date:
                    if force:
                        _log.warning('step %s already completed at %s, continuing anyway', step, date)
                    elif fail:
                        _log.error('step %s already completed at %s', step, date)
                        raise RuntimeError('step {} already completed'.format(step))
                    else:
                        _log.info('step %s already completed at %s', step, date)
                        return False
                else:
                    _log.warning('step %s already started, did it fail?', step)
            cur.execute('''
                INSERT INTO import_status (step)
                VALUES (%s)
                ON CONFLICT (step)
                DO UPDATE SET started_at = now(), finished_at = NULL
            ''', [step])

    return True


def finish(step, dbc=None):
    _log.debug('finishing step %s')
    with database(dbc=dbc, autocommit=True) as db:
        with db.cursor() as cur:
            cur.execute('''
                UPDATE import_status
                SET finished_at = now()
                WHERE step = %s
            ''', [step])
            ct = cur.rowcount
            if ct != 1:
                raise RuntimeError("couldn't update step!")
