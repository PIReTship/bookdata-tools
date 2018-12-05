import logging
from invoke import task

import support as s

_log = logging.getLogger(__name__)


@task(s.build, s.init, name='import')
def import_loc(c, force=False):
    "Import the LOC data"
    s.start('loc', force=force)
    _log.info('initializing LOC schema')
    c.run('psql -f loc-schema.sql')
    loc = s.data_dir / 'LOC'
    files = list(loc.glob('BooksAll.2014.part*.xml.gz'))
    _log.info('importing LOC data from', len(files), 'files')
    s.pipeline([
        [s.bin_dir / 'parse-marc'] + files,
        ['psql', '-c', '\\copy loc_marc_field FROM STDIN']
    ])
    s.finish('loc')

@task(s.init)
def index(c, force=False):
    "Index LOC data"
    s.check_prereq('loc')
    s.start('loc-index', force=force)
    _log.info('building LOC indexes')
    c.run('psql -af loc-index.sql')
    s.finish('loc-index')
