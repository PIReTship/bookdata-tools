from invoke import task

import support as s


@task(s.init, s.build, name='import')
def import_viaf(c, date='20181104', force=False):
    "Import VIAF data"
    s.start('viaf', force=force)
    print('initializing VIAF schema')
    c.run('psql -f viaf-schema.sql')
    infile = s.data_dir / f'viaf-{date}-clusters-marc21.xml.gz'
    print('importing VIAF data from %s', infile)

    s.pipeline([
        [s.bin_dir / 'parse-marc', '--line-mode', infile],
        ['psql', '-c', '\\copy viaf_marc_field FROM STDIN']
    ])
    s.finish('viaf')

@task(s.init)
def index(c, force=False):
    "Index VIAF data"
    s.check_prereq('viaf')
    s.start('viaf-index', force=force)
    print('building VIAF indexes')
    c.run('psql -af viaf-index.sql')
    s.finish('viaf-index')
