import os
import time
from typing import NamedTuple, List
from more_itertools import peekable
import re
from pathlib import Path
import subprocess as sp
from invoke import task
import psycopg2
import psycopg2.errorcodes
import logging
import inspect
import ast
from datetime import timedelta

_log = logging.getLogger(__name__)

data_dir = Path('data')
tgt_dir = Path('target')
bin_dir = tgt_dir / 'release'
bdtool = bin_dir / 'bookdata'

numspaces = dict(work=100000000, edition=200000000, rec=300000000,
                 gr_work=400000000, gr_book=500000000,
                 loc_instance=600000000, loc_work=700000000,
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


def psql(c, script, staged=False):
    if staged:
        with open(script, encoding='utf8') as f:
            parsed = SqlScript(f)
        with database() as dbc:
            parsed.execute(dbc)
    else:
        _log.info('running script %s', script)
        c.run(f'psql -v ON_ERROR_STOP=on -f {script}')


@task
def init(c):
    "Make sure initial database structure are in place"
    try:
        is_initialized = not start('init', fail=False)
    except psycopg2.Error as e:
        _log.warning('PostgreSQL error: %s', e)
        _log.info('Will try to initialize database')
        is_initialized = False

    if not is_initialized:
        psql(c, 'common-schema.sql')
        finish('init')


@task
def build(c, debug=False):
    "Compile the Rust support executables"
    if debug:
        _log.info('compiling support executables in debug mode')
        c.run('cargo build')
    else:
        _log.info('compiling support executables')
        c.run('cargo build --release')


@task
def clean(c):
    "Clean up intermediate & generated files"
    _log.info('cleaning Rust build')
    c.run('cargo clean')
    _log.info('cleaning cluster CSV')
    for f in data_dir.glob('*clusters.csv'):
        _log.debug('rm %s', f)
        f.unlink()
    for f in data_dir.glob('*-edges.csv.gz'):
        _log.debug('rm %s', f)
        f.unlink()
    for f in data_dir.glob('*-isbns.csv.gz'):
        _log.debug('rm %s', f)
        f.unlink()


@task
def test(c, debug=False):
    "Run tests on the import & support code."
    if debug:
        _log.info('testing support executables in debug mode')
        c.run('cargo test')
    else:
        _log.info('testing support executables')
        c.run('cargo test --release')


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
    def __init__(self, autocommit=False, dbc=None):
        self.autocommit = autocommit
        self.connection = dbc
        self.need_close = False

    def __enter__(self):
        if self.connection is None:
            _log.debug('connecting to database')
            self.connection = psycopg2.connect("")
            self.need_close = True

            if self.autocommit:
                self.connection.autocommit = True

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
                        _log.warning('step %s already completed at %s, continuing anyway',
                                     step, date)
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
                RETURNING finished_at - started_at
            ''', [step])
            row = cur.fetchone()
            if row is None:
                raise RuntimeError("couldn't update step!")
            elapsed, = row
            _log.info('finished step %s in %s', step, elapsed)


class ScriptChunk(NamedTuple):
    label: str
    allowed_errors: List[str]
    src: str
    use_transaction: bool = True


class SqlScript:
    """
    Class for processing & executing SQL scripts.
    """

    _sep_re = re.compile(r'^---\s*(?P<inst>.*)')
    _icode_re = re.compile(r'#(?P<code>\w+)\s*(?P<args>.*\S)?\s*$')

    chunks: List[ScriptChunk]

    def __init__(self, file):
        if hasattr(file, 'read'):
            self._parse(peekable(file))
        else:
            with open(file, 'r', encoding='utf8') as f:
                self._parse(peekable(f))

    def _parse(self, lines):
        self.chunks = []
        next_chunk = self._parse_chunk(lines, len(self.chunks) + 1)
        while next_chunk is not None:
            if next_chunk:
                self.chunks.append(next_chunk)
            next_chunk = self._parse_chunk(lines, len(self.chunks) + 1)

    @classmethod
    def _parse_chunk(cls, lines: peekable, n: int):
        qlines = []

        chunk = cls._read_header(lines)
        qlines = cls._read_query(lines)

        # end of file, do we have a chunk?
        if qlines:
            if chunk.label is None:
                chunk = chunk._replace(label=f'Step {n}')
            return chunk._replace(src='\n'.join(qlines))
        elif qlines is not None:
            return False  # empty chunk

    @classmethod
    def _read_header(cls, lines: peekable):
        label = None
        errs = []
        tx = True

        line = lines.peek(None)
        while line is not None:
            hm = cls._sep_re.match(line)
            if hm is None:
                break

            next(lines)  # eat line
            line = lines.peek(None)

            inst = hm.group('inst')
            cm = cls._icode_re.match(inst)
            if cm is None:
                continue
            code = cm.group('code')
            args = cm.group('args')
            if code == 'step':
                label = args
            elif code == 'allow':
                err = getattr(psycopg2.errorcodes, args.upper())
                _log.debug('step allows error %s (%s)', args, err)
                errs.append(err)
            elif code == 'notx':
                _log.debug('chunk will run outside a transaction')
                tx = False
            else:
                _log.error('unrecognized query instruction %s', code)
                raise ValueError(f'invalid query instruction {code}')

        return ScriptChunk(label=label, allowed_errors=errs, src=None,
                           use_transaction=tx)

    @classmethod
    def _read_query(cls, lines: peekable):
        qls = []

        line = lines.peek(None)
        while line is not None and not cls._sep_re.match(line):
            qls.append(next(lines))
            line = lines.peek(None)

        # trim lines
        while qls and not qls[0].strip():
            qls.pop(0)
        while qls and not qls[-1].strip():
            qls.pop(-1)

        if qls or line is not None:
            return qls
        else:
            return None  # end of file

    def execute(self, dbc):
        for step in self.chunks:
            start = time.perf_counter()
            _log.info('Running ‘%s’', step.label)
            _log.debug('Query: %s', step.src)
            if step.use_transaction:
                with dbc, dbc.cursor() as cur:
                    self._run_query(step, dbc, cur, True)
            else:
                with database(autocommit=True) as db2, db2.cursor() as cur:
                    self._run_query(step, db2, cur, False)

            elapsed = time.perf_counter() - start
            elapsed = timedelta(seconds=elapsed)
            _log.info('Finished ‘%s’ in %s', step.label, elapsed)

    def _run_query(self, step, dbc, cur, commit):
        try:
            cur.execute(step.src)
            if commit:
                dbc.commit()
        except psycopg2.Error as e:
            if e.pgcode in step.allowed_errors:
                _log.info('Failed with acceptable error %s (%s)',
                          e.pgcode, psycopg2.errorcodes.lookup(e.pgcode))
            else:
                _log.error('%s failed: %s', step.label, e)
                if e.pgerror:
                    _log.info('Query diagnostics:\n%s', e.pgerror)
                raise e


def _get_tasks(ns):
    for t in ns.tasks.values():
        yield (t.name, t)
    for cn, c in ns.collections.items():
        yield from ((f'{cn}.{tn}', t) for (tn, t) in _get_tasks(c))


def _get_task_stage(task):
    func = task.body
    _re = re.compile(r"\s*s\.finish\('(?P<step>.*)'[\),]")
    lines, n = inspect.getsourcelines(func)
    for line in lines:
        m = _re.match(line)
        if m:
            return m.group('step')


def get_steps(ns):
    steps = {}

    for name, task in _get_tasks(ns):
        _log.debug('looking for steps in %s', task)
        step = _get_task_stage(task)
        if step:
            _log.debug('found step %s', step)
            steps[step] = name

    return steps
