import logging
from invoke import task

import support as s

_log = logging.getLogger(__name__)


@task(s.build, s.init, name='import')
def import_loc(c, force=False):
    "Import the LOC MDS data"
    s.start('loc', force=force)
    _log.info('initializing LOC schema')
    s.psql(c, 'loc-schema.sql')
    loc = s.data_dir / 'LOC'
    files = list(loc.glob('BooksAll.2014.part*.xml.gz'))
    _log.info('importing LOC data from', len(files), 'files')
    s.pipeline([
        [s.bin_dir / 'parse-marc'] + files,
        ['psql', '-c', '\\copy loc_marc_field FROM STDIN']
    ])
    s.finish('loc')

@task(s.build, s.init)
def import_names(c, force=False):
    "Import the LOC MDS name data"
    s.start('loc-mds-names', force=force)
    _log.info('initializing LOC schema')
    s.psql(c, 'loc-name-schema.sql')
    loc = s.data_dir / 'LOC'
    names = loc / 'Names.2014.combined.xml.gz'
    _log.info('importing LOC data from %s', loc)
    s.pipeline([
        [s.bin_dir / 'parse-marc', names],
        ['psql', '-c', '\\copy locmds_name_marc_field FROM STDIN']
    ])
    s.finish('loc-mds-names')

@task(s.init)
def index(c, force=False):
    "Index LOC MDS data"
    s.check_prereq('loc')
    s.start('loc-index', force=force)
    _log.info('building LOC indexes')
    s.psql(c, 'loc-index.sql')
    s.finish('loc-index')

@task(s.init)
def index_names(c, force=False):
    "Index LOC MDS name data"
    s.check_prereq('loc')
    s.start('loc-mds-names-index', force=force)
    _log.info('building LOC indexes')
    s.psql(c, 'loc-name-index.sql')
    s.finish('loc-mds-names-index')
