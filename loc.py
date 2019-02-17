import logging
from invoke import task

import support as s

_log = logging.getLogger(__name__)


@task(s.init)
def init(c, force=False):
    if s.start('loc-mds-init', force=force, fail=False):
        _log.info('initializing LOC schema')
        s.psql(c, 'loc-mds-schema.sql')
        s.finish('loc-mds-init')
    else:
        _log.info('LOC schema initialized')


@task(s.init)
def init_id(c, force=False):
    if s.start('loc-id-init', force=force, fail=False):
        _log.info('initializing LOC schema')
        s.psql(c, 'loc-id-schema.sql')
        s.finish('loc-id-init')
    else:
        _log.info('LOC schema initialized')


@task(s.build, s.init, init)
def import_books(c, force=False):
    "Import the LOC MDS data"
    s.start('loc-mds-books', force=force)
    loc = s.data_dir / 'LOC'
    files = list(loc.glob('BooksAll.2014.part*.xml.gz'))
    _log.info('importing LOC data from', len(files), 'files')
    s.pipeline([
        [s.bin_dir / 'parse-marc'] + files,
        ['psql', '-c', '\\copy locmds.book_marc_field FROM STDIN']
    ])
    s.finish('loc-mds-books')

@task(s.build, s.init, init)
def import_names(c, force=False):
    "Import the LOC MDS name data"
    s.start('loc-mds-names', force=force)
    loc = s.data_dir / 'LOC'
    names = loc / 'Names.2014.combined.xml.gz'
    _log.info('importing LOC data from %s', loc)
    s.pipeline([
        [s.bin_dir / 'parse-marc', names],
        ['psql', '-c', '\\copy locmds.name_marc_field FROM STDIN']
    ])
    s.finish('loc-mds-names')


@task(s.init)
def index_books(c, force=False):
    "Index LOC MDS books data"
    s.check_prereq('loc-mds-books')
    s.start('loc-mds-book-index', force=force)
    _log.info('building LOC indexes')
    s.psql(c, 'loc-mds-index-books.sql')
    s.finish('loc-mds-book-index')


@task(s.init)
def index_names(c, force=False):
    "Index LOC MDS name data"
    s.check_prereq('loc-mds-names')
    s.start('loc-mds-name-index', force=force)
    _log.info('building LOC name indexes')
    s.psql(c, 'loc-mds-index-names.sql')
    s.finish('loc-mds-name-index')


@task(s.build, s.init, init_id)
def import_id_auth(c, force=False, convert_only=False, convert=True):
    s.start('loc-id-names', force=force)
    loc = s.data_dir / 'LOC'
    auth = loc / 'authoritiesnames.nt.both.zip'
    auth_dir = loc / 'authorities'
    if convert:
        _log.info('converting authority ntriples to PSQL')
        s.pipeline([
            [s.bin_dir / 'import-ntriples', '--db-schema', 'locid', auth, auth_dir]
        ])
    if convert_only:
        return

    _log.info('importing nodes')
    s.pipeline([
        [s.bin_dir / 'pcat', '-d', 'snappy', auth_dir / 'nodes.snappy'],
        ['psql', '-c', '\\copy locid.nodes FROM STDIN']
    ])

    _log.info('importing literals')
    s.pipeline([
        [s.bin_dir / 'pcat', '-d', 'snappy', auth_dir / 'literals.snappy'],
        ['psql', '-c', '\\copy locid.literals FROM STDIN']
    ])

    _log.info('importing triples')
    s.pipeline([
        [s.bin_dir / 'pcat', '-d', 'snappy', auth_dir / 'triples.snappy'],
        ['psql', '-c', '\\copy locid.auth_triple FROM STDIN']
    ])
    s.finish('loc-id-names')


@task(s.build, s.init, init_id)
def import_id_work(c, force=False, convert_only=False, convert=True):
    s.start('loc-id-works', force=force)
    loc = s.data_dir / 'LOC'
    auth = loc / 'bibframeworks.nt.zip'
    auth_dir = loc / 'works'
    if convert:
        _log.info('converting BIBFRAME ntriples to PSQL')
        s.pipeline([
            [s.bin_dir / 'import-ntriples', '--db-schema', 'locid', auth, auth_dir]
        ])
    if convert_only:
        return

    _log.info('importing nodes')
    s.pipeline([
        [s.bin_dir / 'pcat', '-d', 'snappy', auth_dir / 'nodes.snappy'],
        ['psql', '-c', '\\copy locid.nodes FROM STDIN']
    ])

    _log.info('importing literals')
    s.pipeline([
        [s.bin_dir / 'pcat', '-d', 'snappy', auth_dir / 'literals.snappy'],
        ['psql', '-c', '\\copy locid.literals FROM STDIN']
    ])

    _log.info('importing triples')
    s.pipeline([
        [s.bin_dir / 'pcat', '-d', 'snappy', auth_dir / 'triples.snappy'],
        ['psql', '-c', '\\copy locid.work_triple FROM STDIN']
    ])
    s.finish('loc-id-works')
