from invoke import task

import support as s


@task
def init(c):
    "Initialize the LOC schema"
    c.run('psql -f loc-schema.sql')


@task(s.build, init, name='import')
def import_loc(c):
    "Import the LOC data"
    loc = s.data_dir / 'LOC'
    files = list(loc.glob('BooksAll.2014.part*.xml.gz'))
    print('importing LOC data from', len(files), 'files')
    s.pipeline([
        [s.bin_dir / 'parse-marc'] + files,
        ['psql', '-c', '\\copy loc_marc_field FROM STDIN']
    ])
