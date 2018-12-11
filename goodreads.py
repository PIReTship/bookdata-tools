import logging

from invoke import task

import support as s

_log = logging.getLogger(__name__)


@task(s.init, s.build, name='import')
def import_data(c, force=False):
    "Import GoodReads rating and book data"
    s.start('gr-data', force=force)
    _log.info('Resetting GoodReads schema')
    c.run('psql -f gr-schema.sql')
    _log.info('Importing GoodReads books')
    s.pipeline([
      [s.bin_dir / 'clean-json', s.data_dir / 'goodreads_books.json.gz'],
      ['psql', '-c', '\\copy gr_raw_book (gr_book_data) FROM STDIN']
    ])
    _log.info('Importing GoodReads works')
    s.pipeline([
      [s.bin_dir / 'clean-json', s.data_dir / 'goodreads_book_works.json.gz'],
      ['psql', '-c', '\\copy gr_raw_work (gr_work_data) FROM STDIN']
    ])
    _log.info('Importing GoodReads authors')
    s.pipeline([
      [s.bin_dir / 'clean-json', s.data_dir / 'goodreads_book_authors.json.gz'],
      ['psql', '-c', '\\copy gr_raw_author (gr_author_data) FROM STDIN']
    ])
    _log.info('Importing GoodReads interactions')
    s.pipeline([
      [s.bin_dir / 'clean-json', s.data_dir / 'goodreads_interactions.json.gz'],
      ['psql', '-c', '\\copy gr_raw_interaction (gr_int_data) FROM STDIN']
    ])
    s.finish('gr-data')


@task(s.init)
def index_books(c, force=False):
    "Index GoodReads book data"
    s.check_prereq('gr-data')
    s.start('gr-index-books', force=force)
    _log.info('building GoodReads indexes')
    c.run('psql -af gr-index-books.sql')
    s.finish('gr-index-books')


@task(s.init)
def index_ratings(c, force=False):
    "Index GoodReads rating/interaction data"
    s.check_prereq('gr-data')
    s.check_prereq('cluster')
    s.start('gr-index-ratings', force=force)
    _log.info('building GoodReads indexes')
    c.run('psql -af gr-index-ratings.sql')
    s.finish('gr-index-ratings')
