import logging

from invoke import task

import support as s

_log = logging.getLogger(__name__)


@task(s.init, s.build, name='import')
def import_data(c, force=False):
    "Import GoodReads rating and book data"
    s.start('gr-data', force=force)
    _log.info('Resetting GoodReads schema')
    s.psql(c, 'gr-schema.sql')
    _log.info('Importing GoodReads books')
    s.pipeline([
      [s.bin_dir / 'import-json', '--truncate', 'goodreads', 'book', s.data_dir / 'goodreads_books.json.gz']
    ])
    _log.info('Importing GoodReads works')
    s.pipeline([
      [s.bin_dir / 'import-json', '--truncate', 'goodreads', 'work', s.data_dir / 'goodreads_book_works.json.gz']
    ])
    _log.info('Importing GoodReads authors')
    s.pipeline([
      [s.bin_dir / 'import-json', '--truncate', 'goodreads', 'author', s.data_dir / 'goodreads_book_authors.json.gz']
    ])
    _log.info('Importing GoodReads interactions')
    s.pipeline([
      [s.bin_dir / 'import-json', '--truncate', 'goodreads', 'interaction', s.data_dir / 'goodreads_interactions.json.gz']
    ])
    s.finish('gr-data')


@task(s.init)
def index_books(c, force=False):
    "Index GoodReads book data"
    s.check_prereq('gr-data')
    s.start('gr-index-books', force=force)
    _log.info('building GoodReads indexes')
    s.psql(c, 'gr-index-books.sql', True)
    s.finish('gr-index-books')


@task(s.init)
def index_ratings(c, force=False):
    "Index GoodReads rating/interaction data"
    s.check_prereq('gr-data')
    s.check_prereq('cluster')
    s.start('gr-index-ratings', force=force)
    _log.info('building GoodReads indexes')
    s.psql(c, 'gr-index-ratings.sql', True)
    s.finish('gr-index-ratings')
