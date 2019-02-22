import logging
from invoke import task

import support as s

_log = logging.getLogger(__name__)


@task(s.init)
def init(c, force=False):
    "Initialize the OpenLibrary schema"
    if s.start('ol-init', fail=False, force=force):
        _log.info('initializing OpenLibrary schema')
        s.psql(c, 'ol-schema.sql')
        s.finish('ol-init')


@task(s.build, s.init)
def import_authors(c, date='2018-10-31', force=False):
    "Import OpenLibrary authors"
    s.check_prereq('ol-init')
    s.start('ol-authors', force=force)
    infile = s.data_dir / f'ol_dump_authors_{date}.txt.gz'
    _log.info('importing OL authors from', infile)

    s.pipeline([
        [s.bin_dir / 'import-json', '--truncate', 'openlib', 'author', infile]
    ])
    s.finish('ol-authors')


@task(s.build, s.init)
def import_editions(c, date='2018-10-31', force=False):
    "Import OpenLibrary editions"
    s.check_prereq('ol-init')
    s.start('ol-editions', force=force)
    infile = s.data_dir / f'ol_dump_editions_{date}.txt.gz'
    _log.info('importing OL editions from', infile)

    s.pipeline([
        [s.bin_dir / 'import-json', '--truncate', 'openlib', 'edition', infile]
    ])
    s.finish('ol-editions')


@task(s.build, s.init)
def import_works(c, date='2018-10-31', force=False):
    "Import OpenLibrary works"
    s.check_prereq('ol-init')
    s.start('ol-works', force=force)
    infile = s.data_dir / f'ol_dump_works_{date}.txt.gz'

    s.pipeline([
        [s.bin_dir / 'import-json', '--truncate', 'openlib', 'work', infile]
    ])
    s.finish('ol-works')

@task(s.init)
def index(c, force=False):
    "Index OpenLibrary data"
    s.check_prereq('ol-works')
    s.check_prereq('ol-editions')
    s.check_prereq('ol-authors')
    s.start('ol-index', force=force)
    _log.info('building OpenLibrary indexes')
    s.psql(c, 'ol-index.sql')
    s.finish('ol-index')
