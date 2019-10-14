import os
import sys
import re
import time
import logging
import hashlib
import threading
from pathlib import Path
from contextlib import contextmanager
from datetime import timedelta
from typing import NamedTuple, List
from docopt import docopt
from natural.date import compress as compress_date

import pandas as pd

from more_itertools import peekable
import psycopg2, psycopg2.errorcodes
from psycopg2 import sql
import sqlparse

_log = logging.getLogger(__name__)

# Meta-schema for storing stage and file status in the database
_ms_path = Path(__file__).parent.parent / 'schemas' / 'meta-schema.sql'
meta_schema = _ms_path.read_text()


def db_url():
    "Get the URL to connect to the database."
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


@contextmanager
def connect():
    "Connect to a database. This context manager yields the connection, and closes it when exited."
    _log.info('connecting to %s', db_url())
    conn = psycopg2.connect(db_url())
    try:
        yield conn
    finally:
        conn.close()


def hash_and_record_file(cur, path, stage=None):
    """
    Compute the checksum of a file and record it in the database.
    """
    h = hashlib.md5()
    with open(path, 'rb') as f:
        data = f.read(8192 * 4)
        while data:
            h.update(data)
            data = f.read(8192 * 4)
    hash = h.hexdigest()
    path = Path(path).as_posix()
    _log.info('recording file %s with hash %s', path, hash)
    record_file(cur, path, hash, stage)
    return hash


def begin_stage(cur, stage):
    """
    Record that a stage is beginning.
    """
    if hasattr(cur, 'cursor'):
        # this is a connection
        with cur, cur.cursor() as c:
            return begin_stage(c, stage)
    _log.info('starting or resetting stage %s', stage)
    cur.execute('''
        INSERT INTO stage_status (stage_name)
        VALUES (%s)
        ON CONFLICT (stage_name)
        DO UPDATE SET started_at = now(), finished_at = NULL, stage_key = NULL
    ''', [stage])
    cur.execute('DELETE FROM stage_file WHERE stage_name = %s', [stage])
    cur.execute('DELETE FROM stage_dep WHERE stage_name = %s', [stage])


def record_dep(cur, stage, dep):
    """
    Record a dependency for a stage.
    """
    if hasattr(cur, 'cursor'):
        # this is a connection
        with cur, cur.cursor() as c:
            return record_dep(c, stage, dep)

    _log.info('recording dep %s -> %s', stage, dep);
    cur.execute('''
        INSERT INTO stage_dep (stage_name, dep_name, dep_key)
        SELECT %s, stage_name, stage_key
        FROM stage_status WHERE stage_name = %s
        RETURNING dep_name, dep_key
    ''', [stage, dep])
    return cur.fetchall()


def record_file(cur, file, hash, stage=None):
    """
    Record a file and optionally associate it with a stage.
    """
    if hasattr(cur, 'cursor'):
        # this is a connection
        with cur, cur.cursor() as c:
            return record_file(c, stage)
    _log.info('recording checksum %s for file %s', hash, file)
    cur.execute("""
        INSERT INTO source_file (filename, checksum)
        VALUES (%(file)s, %(hash)s)
        ON CONFLICT (filename)
        DO UPDATE SET checksum = %(hash)s, reg_time = NOW()
        """, {'file': file, 'hash': hash})
    if stage is not None:
        cur.execute("INSERT INTO stage_file (stage_name, filename) VALUES (%s, %s)", [stage, file])


def end_stage(cur, stage, key=None):
    """
    Record that an import stage has finished.

    Args:
        cur(psycopg2.connection or psycopg2.cursor): the database connection to use.
        stage(string): the name of the stage.
        key(string or None): the key (checksum or other key) to record.
    """
    if hasattr(cur, 'cursor'):
        # this is a connection
        with cur, cur.cursor() as c:
            return end_stage(c, stage, key)
    _log.info('finishing stage %s', stage)
    cur.execute('''
        UPDATE stage_status
        SET finished_at = NOW(), stage_key = coalesce(%(key)s, stage_key)
        WHERE stage_name = %(stage)s
    ''', {'stage': stage, 'key': key})


def _tokens(s, start=-1, skip_ws=True, skip_cm=True):
    i, t = s.token_next(start, skip_ws=skip_ws, skip_cm=skip_cm)
    while t is not None:
        yield t
        i, t = s.token_next(i, skip_ws=skip_ws, skip_cm=skip_cm)


def describe_statement(s):
    "Describe an SQL statement.  This utility function is used to summarize statements."
    label = s.get_type()
    li, lt = s.token_next(-1, skip_cm=True)
    if lt is None:
        return None
    if lt and lt.ttype == sqlparse.tokens.DDL:
        # DDL - build up!
        parts = []
        first = True
        skipping = False
        for t in _tokens(s, li):
            if not first:
                if isinstance(t, sqlparse.sql.Identifier) or isinstance(t, sqlparse.sql.Function):
                    parts.append(t.normalized)
                    break
                elif t.ttype != sqlparse.tokens.Keyword:
                    break

            first = False

            if t.normalized == 'IF':
                skipping = True

            if not skipping:
                parts.append(t.normalized)

        label = label + ' ' + ' '.join(parts)
    elif label == 'UNKNOWN':
        ls = []
        for t in _tokens(s):
            if t.ttype == sqlparse.tokens.Keyword:
                ls.append(t.normalized)
            else:
                break
        if ls:
            label = ' '.join(ls)

        name = s.get_real_name()
        if name:
            label += f' {name}'

    return label


def is_empty(s):
    "check if an SQL statement is empty"
    lt = s.token_first(skip_cm=True, skip_ws=True)
    return lt is None


class ScriptChunk(NamedTuple):
    "A single chunk of an SQL script."
    label: str
    allowed_errors: List[str]
    src: str
    use_transaction: bool = True

    @property
    def statements(self):
        return [s for s in sqlparse.parse(self.src) if not is_empty(s)]


class SqlScript:
    """
    Class for processing & executing SQL scripts with the following features ``psql``
    does not have:

    * Splitting the script into (named) steps, to commit chunks in transactions
    * Recording metadata (currently just dependencies) for the script
    * Allowing chunks to fail with specific errors

    The last feature is to help with writing _idempotent_ scripts: by allowing a chunk
    to fail with a known error (e.g. creating a constraint that already exists), you
    can write a script that can run cleanly even if it has already been run.

    Args:
        file: the path to the SQL script to read.
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
        self.deps = self._parse_script_header(lines)
        next_chunk = self._parse_chunk(lines, len(self.chunks) + 1)
        while next_chunk is not None:
            if next_chunk:
                self.chunks.append(next_chunk)
            next_chunk = self._parse_chunk(lines, len(self.chunks) + 1)

    @classmethod
    def _parse_script_header(cls, lines):
        deps = []

        line = lines.peek(None)
        while line is not None:
            hm = cls._sep_re.match(line)
            if hm is None:
                break

            inst = hm.group('inst')
            cm = cls._icode_re.match(inst)
            if cm is None:
                next(lines)  # eat line
                continue

            code = cm.group('code')
            args = cm.group('args')
            if code == 'dep':
                deps.append(args)
                next(lines)  # eat line
            else:  # any other code, we're out of header
                break

            line = lines.peek(None)

        return deps

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

    def execute(self, dbc, transcript=None):
        """
        Execute the SQL script.

        Args:
            dbc: the database connection.
            transcript: a file to receive the run transcript.
        """
        all_st = time.perf_counter()
        for step in self.chunks:
            start = time.perf_counter()
            _log.info('Running ‘%s’', step.label)
            if transcript is not None:
                print('CHUNK', step.label, file=transcript)
            if step.use_transaction:
                with dbc, dbc.cursor() as cur:
                    self._run_step(step, dbc, cur, True, transcript)
            else:
                ac = dbc.autocommit
                try:
                    dbc.autocommit = True
                    with dbc.cursor() as cur:
                        self._run_step(step, dbc, cur, False, transcript)
                finally:
                    dbc.autocommit = ac

            elapsed = time.perf_counter() - start
            elapsed = timedelta(seconds=elapsed)
            print('CHUNK ELAPSED', elapsed, file=transcript)
            _log.info('Finished ‘%s’ in %s', step.label, compress_date(elapsed))
        elapsed = time.perf_counter() - all_st
        elasped = timedelta(seconds=elapsed)
        _log.info('Script completed in %s', compress_date(elapsed))

    def describe(self):
        for dep in self.deps:
            _log.info('Dependency ‘%s’', dep)
        for step in self.chunks:
            _log.info('Chunk ‘%s’', step.label)
            for s in step.statements:
                _log.info('Statement %s', describe_statement(s))

    def _run_step(self, step, dbc, cur, commit, transcript):
        try:
            for sql in step.statements:
                start = time.perf_counter()
                _log.debug('Executing %s', describe_statement(sql))
                _log.debug('Query: %s', sql)
                if transcript is not None:
                    print('STMT', describe_statement(sql), file=transcript)
                cur.execute(str(sql))
                elapsed = time.perf_counter() - start
                elapsed = timedelta(seconds=elapsed)
                rows = cur.rowcount
                if transcript is not None:
                    print('ELAPSED', elapsed, file=transcript)
                if rows is not None and rows >= 0:
                    if transcript is not None:
                        print('ROWS', rows, file=transcript)
                    _log.info('finished %s in %s (%d rows)', describe_statement(sql),
                              compress_date(elapsed), rows)
                else:
                    _log.info('finished %s in %s (%d rows)', describe_statement(sql),
                              compress_date(elapsed), rows)
            if commit:
                dbc.commit()
        except psycopg2.Error as e:
            if e.pgcode in step.allowed_errors:
                _log.info('Failed with acceptable error %s (%s)',
                          e.pgcode, psycopg2.errorcodes.lookup(e.pgcode))
                if transcript is not None:
                    print('ERROR', e.pgcode, psycopg2.errorcodes.lookup(e.pgcode), file=transcript)
            else:
                _log.error('Error in "%s" %s: %s: %s',
                           step.label, describe_statement(sql),
                           psycopg2.errorcodes.lookup(e.pgcode), e)
                if e.pgerror:
                    _log.info('Query diagnostics:\n%s', e.pgerror)
                raise e


class _LoadThread(threading.Thread):
    """
    Thread worker for copying database results to a stream we can read.
    """
    def __init__(self, dbc, query, dir='out'):
        super().__init__()
        self.database = dbc
        self.query = query
        rfd, wfd = os.pipe()
        self.reader = os.fdopen(rfd)
        self.writer = os.fdopen(wfd, 'w')
        self.chan = self.writer if dir == 'out' else self.reader

    def run(self):
        with self.chan, self.database.cursor() as cur:
            cur.copy_expert(self.query, self.chan)


def load_table(dbc, query):
    """
    Load a query into a Pandas data frame.

    This is substantially more efficient than Pandas ``read_sql``, because it directly
    streams CSV data from the database instead of going through SQLAlchemy.
    """
    cq = sql.SQL('COPY ({}) TO STDOUT WITH CSV HEADER')
    q = sql.SQL(query)
    thread = _LoadThread(dbc, cq.format(q))
    thread.start()
    data = pd.read_csv(thread.reader)
    thread.join()
    return data


def save_table(dbc, table, data: pd.DataFrame):
    """
    Save a table from a Pandas data frame.

    This is substantially more efficient than Pandas ``read_sql``, because it directly
    streams CSV data from the database instead of going through SQLAlchemy.
    """
    cq = sql.SQL('COPY {} FROM STDIN WITH CSV')
    thread = _LoadThread(dbc, cq.format(table), 'in')
    thread.start()
    data.to_csv(thread.writer, header=False, index=False)
    thread.writer.close()
    thread.join()
