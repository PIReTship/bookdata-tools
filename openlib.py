from invoke import task

import support as s

@task
def init(c):
    "Initialize the OpenLibrary schema"
    print('initializing OpenLibrary schema')
    c.run('psql -f ol-schema.sql')


@task(s.build)
def import_authors(c, date='2018-10-31'):
    "Import OpenLibrary authors"
    infile = s.data_dir / f'ol_dump_authors_{date}.txt.gz'

    s.pipeline([
        [s.bin_dir / 'clean-openlib', infile],
        ['psql', '-c', '\\copy ol_author (author_key, author_data) FROM STDIN']
    ])


@task(s.build)
def import_editions(c, date='2018-10-31'):
    "Import OpenLibrary editions"
    infile = s.data_dir / f'ol_dump_editions_{date}.txt.gz'

    s.pipeline([
        [s.bin_dir / 'clean-openlib', infile],
        ['psql', '-c', '\\copy ol_edition (edition_key, edition_data) FROM STDIN']
    ])


@task(s.build)
def import_works(c, date='2018-10-31'):
    "Import OpenLibrary works"
    infile = s.data_dir / f'ol_dump_works_{date}.txt.gz'

    s.pipeline([
        [s.bin_dir / 'clean-openlib', infile],
        ['psql', '-c', '\\copy ol_work (work_key, work_data) FROM STDIN']
    ])
