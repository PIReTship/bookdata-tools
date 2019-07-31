import logging
from invoke import task

import support as s

_log = logging.getLogger(__name__)

dft_date = '2018-10-31'


@task(s.init)
def init(c, force=False):
    "Initialize the OpenLibrary schema"
    if s.start('ol-init', fail=False, force=force):
        _log.info('initializing OpenLibrary schema')
        s.psql(c, 'ol-schema.sql')
        s.finish('ol-init')


@task(s.build, s.init)
def import_authors(c, date=dft_date, force=False):
    "Import OpenLibrary authors"
    s.check_prereq('ol-init')
    s.start('ol-authors', force=force)
    infile = s.data_dir / f'ol_dump_authors_{date}.txt.gz'
    _log.info('importing OL authors from %s', infile)

    s.pipeline([
        [s.bdtool, 'import-json', '--truncate', 'openlib', 'author', infile]
    ])
    s.finish('ol-authors')


@task(s.build, s.init)
def import_editions(c, date=dft_date, force=False):
    "Import OpenLibrary editions"
    s.check_prereq('ol-init')
    s.start('ol-editions', force=force)
    infile = s.data_dir / f'ol_dump_editions_{date}.txt.gz'
    _log.info('importing OL editions from %s', infile)

    s.pipeline([
        [s.bdtool, 'import-json', '--truncate', 'openlib', 'edition', infile]
    ])
    s.finish('ol-editions')


@task(s.build, s.init)
def import_works(c, date=dft_date, force=False):
    "Import OpenLibrary works"
    s.check_prereq('ol-init')
    s.start('ol-works', force=force)
    infile = s.data_dir / f'ol_dump_works_{date}.txt.gz'
    _log.info('importing works from %s', infile)

    s.pipeline([
        [s.bdtool, 'import-json', '--truncate', 'openlib', 'work', infile]
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


@task(s.init, s.build)
def record_files(c, date=dft_date):
    files = [s.data_dir / f'ol_dump_{x}_{date}.txt.gz' for x in ['authors', 'editions', 'works']]
    s.booktool(c, 'hash', *files)
