from invoke import task

import support as s


@task(s.build, s.init, name='import')
def import_loc(c, force=False):
    "Import the LOC data"
    s.start('loc', force=force)
    print('initializing LOC schema')
    c.run('psql -f loc-schema.sql')
    loc = s.data_dir / 'LOC'
    files = list(loc.glob('BooksAll.2014.part*.xml.gz'))
    print('importing LOC data from', len(files), 'files')
    s.pipeline([
        [s.bin_dir / 'parse-marc'] + files,
        ['psql', '-c', '\\copy loc_marc_field FROM STDIN']
    ])
    s.finish('loc')
