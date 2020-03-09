"""
Code for supporting import data tracking and relationships.
"""

import hashlib
import logging
from io import StringIO
from pathlib import Path

from . import db

_log = logging.getLogger(__name__)


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
    cur.execute('DELETE FROM stage_table WHERE stage_name = %s', [stage])


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


def record_tbl(cur, stage, ns, tbl):
    """
    Record a table associated with a stage.
    """
    if hasattr(cur, 'cursor'):
        # this is a connection
        with cur, cur.cursor() as c:
            return record_tbl(c, stage, ns, tbl)

    _log.info('recording table %s -> %s.%s', stage, ns, tbl);
    cur.execute('''
        INSERT INTO stage_table (stage_name, st_ns, st_name)
        VALUES (%s, %s, %s)
    ''', [stage, ns, tbl])
    cur.execute('''
        SELECT oid, kind FROM stage_table_oids WHERE stage_name = %s AND st_ns = %s AND st_name = %s
    ''', [stage, ns, tbl])
    return cur.fetchone()


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


def stage_status(stage, file=None, *, timestamps=False):
    if file is None:
        sf = StringIO()
    else:
        sf = file

    with db.connect() as dbc:
        # initialize database, in case nothing has been run
        with dbc, dbc.cursor() as cur:
            cur.execute(db.meta_schema)

        # get the status
        with dbc, dbc.cursor() as cur:
            cur.execute('''
                SELECT started_at, finished_at, stage_key FROM stage_status WHERE stage_name = %s
            ''', [stage])
            row = cur.fetchone()
            if not row:
                _log.error('stage %s not run', stage)
                sys.exit(2)

            start, end, key = row

            _log.info('stage %s finished at %s', stage, end)
            print('STAGE', stage, file=sf)
            if timestamps:
                print('START', start, file=sf)

            cur.execute('''
                SELECT dep_name, dep_key
                FROM stage_dep
                WHERE stage_name = %s
                ORDER BY dep_name
            ''', [stage])
            for dn, dk in cur:
                print('DEP', dn, dk, file=sf)

            cur.execute('''
                SELECT filename, COALESCE(link.checksum, src.checksum)
                FROM source_file src
                JOIN stage_file link USING (filename)
                WHERE stage_name = %s
                ORDER BY filename
            ''', [stage])
            for fn, fh in cur:
                print('SOURCE', fn, fh, file=sf)

            cur.execute('''
                SELECT st_ns, st_name, oid, kind
                FROM stage_table_oids
                WHERE stage_name = %s
                ORDER BY st_ns, st_name
            ''', [stage])
            for ns, tbl, oid, kind in cur:
                print(f'TABLE {ns}.{tbl} OID {oid} KIND {kind}', file=sf)

            if timestamps:
                print('FINISH', end, file=sf)
            if key:
                print('KEY', key, file=sf)

    if file is None:
        return sf.getvalue()
