from invoke import task

import support as s

@task
def init(c):
    "Initialize the VIAF schema"
    print('initializing VIAF schema')
    c.run('psql -f viaf-schema.sql')


@task(s.build, init, name='import')
def import_viaf(c, date='20181104'):
    "Import VIAF data"
    infile = s.data_dir / f'viaf-{date}-clusters-marc21.xml.gz'
    print('importing VIAF data from %s', infile)

    s.pipeline([
        [s.bin_dir / 'parse-marc', '--line-mode', infile],
        ['psql', '-c', '\\copy viaf_marc_field FROM STDIN']
    ])

