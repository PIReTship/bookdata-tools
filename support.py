import sys
import os
from pathlib import Path
import subprocess as sp
from invoke import task
import psycopg2

data_dir = Path('data')
tgt_dir = Path('target')
bin_dir = tgt_dir / 'release'

@task
def init(c):
    "Make sure initial database structure are in place"
    c.run("psql -f common-schema.sql")


@task
def build(c, debug=False):
    "Compile the Rust support executables"
    global bin_dir
    if debug:
        print('compiling support executables in debug mode')
        c.run('cargo build')
        bin_dir = tgt_dir / 'debug'
    else:
        print('compiling support executables')
        c.run('cargo build --release')


def pipeline(steps, outfile=None):
    last = sp.DEVNULL
    if outfile is not None:
        outfd = os.open(outfile, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o666)
    else:
        outfd = None

    procs = []
    for step in steps[:-1]:
        proc = sp.Popen(step, stdin=last, stdout=sp.PIPE)
        last = proc.stdout
        procs.append(proc)

    proc = sp.Popen(steps[-1], stdin=last, stdout=outfd)
    procs.append(proc)

    for p, s in zip(procs, steps):
        rc = p.wait()
        if rc != 0:
            print(f'{s[0]} exited with code {rc}', file=sys.stderr)
            raise RuntimeError('subprocess failed')


class database:
    def __init__(self, autocommit = False, dbc=None):
        self.autocommit = autocommit
        self.connection = dbc
        self.need_close = False

    def __enter__(self):
        if self.connection is None:
            self.connection = psycopg2.connect("")
            self.need_close = True

            if self.autocommit:
                self.connection.set_session(autocommit=True)

        return self.connection
    
    def __exit__(self, *args):
        if self.need_close:
            self.connection.close()
            self.need_close = False


def check_prereq(step, dbc=None):
    with database(dbc=dbc, autocommit=True) as db:
        with db.cursor() as cur:
            cur.execute('''
                SELECT finished_at FROM import_status 
                WHERE step = %s AND finished_at IS NOT NULL
            ''', [step])
            res = cur.fetchone()
            if not res:
                print('prerequisite step', step, 'not completed', file=sys.stderr)
                raise RuntimeError('prerequisites not met')


def start(step, force=False, dbc=None):
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
                    print('step {} already completed at {}'.format(step, date),
                          file=sys.stderr)
                    if force:
                        print('continuing anyway', file=sys.stderr)
                    else:
                        raise RuntimeError('step {} already completed'.format(step))
                else:
                    print('WARNING: step', step, 'already started, did it fail?')
            cur.execute('''
                INSERT INTO import_status (step)
                VALUES (%s)
                ON CONFLICT (step)
                DO UPDATE started_at = now(), finished_at = NULL
            ''', [step])


def finish(step, dbc=None):
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
