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

@task(s.build, s.init, init)
def import_books(c, force=False):
    "Import the LOC MDS data"
    s.start('loc-mds-books', force=force)
    loc = s.data_dir / 'LOC'
    files = list(loc.glob('BooksAll.2014.part*.xml.gz'))
    _log.info('importing LOC data from %d files', len(files))
    s.pipeline([
        [s.bdtool, 'parse-marc', '--db-schema', 'locmds', '-t', 'book_marc_field', '--truncate'] + files
    ])
    s.finish('loc-mds-books')


@task(s.init)
def index_books(c, force=False):
    "Index LOC MDS books data"
    s.check_prereq('loc-mds-books')
    s.start('loc-mds-book-index', force=force)
    _log.info('building LOC indexes')
    s.psql(c, 'loc-mds-index-books.sql')
    s.finish('loc-mds-book-index')
