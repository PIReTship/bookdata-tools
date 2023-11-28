local bd = import '../lib.jsonnet';

local variants = {
  full: import 'full-interactions.jsonnet',
  simple: import 'simple-interactions.jsonnet',
};

bd.pipeline({
  'scan-book-info': {
    cmd: bd.cmd('goodreads scan books ../data/goodreads/goodreads_books.json.gz'),
    deps: [
      '../src/cli/goodreads',
      '../src/goodreads',
      '../data/goodreads/goodreads_books.json.gz',
    ],
    outs: [
      'gr-book-ids.parquet',
      'gr-book-info.parquet',
      'gr-book-authors.parquet',
      'gr-book-series.parquet',
    ],
  },

  'scan-work-info': {
    cmd: bd.cmd('goodreads scan works ../data/goodreads/goodreads_book_works.json.gz'),
    deps: [
      '../src/cli/goodreads',
      '../src/goodreads',
      '../data/goodreads/goodreads_book_works.json.gz',
    ],
    outs: [
      'gr-work-info.parquet',
    ],
  },

  'scan-book-genres': {
    cmd: bd.cmd('goodreads scan genres ../data/goodreads/goodreads_book_genres_initial.json.gz'),
    deps: [
      '../src/cli/goodreads',
      '../src/goodreads',
      '../data/goodreads/goodreads_book_genres_initial.json.gz',
    ],
    outs: [
      'gr-book-genres.parquet',
      'gr-genres.parquet',
    ],
  },

  'scan-author-info': {
    cmd: bd.cmd('goodreads scan authors ../data/goodreads/goodreads_book_authors.json.gz'),
    deps: [
      '../src/cli/goodreads',
      '../src/goodreads',
      '../data/goodreads/goodreads_book_authors.json.gz',
    ],
    outs: [
      'gr-author-info.parquet',
    ],
  },

  'book-isbn-ids': {
    wdir: '..',
    cmd: bd.cmd('link-isbn-ids -o goodreads/book-isbn-ids.parquet -R book_id -I isbn10 -I isbn13 -I asin goodreads/gr-book-ids.parquet'),
    deps: [
      'src/cli/goodreads',
      'goodreads/gr-book-ids.parquet',
      'book-links/all-isbns.parquet',
    ],
    outs: [
      'goodreads/book-isbn-ids.parquet',
    ],
  },

  'book-links': {
    wdir: '..',
    cmd: bd.cmd('cluster extract-books -o goodreads/gr-book-link.parquet -n book_id --join-file goodreads/gr-book-ids.parquet --join-field work_id GR-B'),
    deps: [
      'goodreads/gr-book-ids.parquet',
      'book-links/cluster-graph-nodes.parquet',
    ],
    outs: [
      'goodreads/gr-book-link.parquet',
    ],
  },

  'work-gender': {
    cmd: bd.cmd('goodreads work-gender'),
    deps: [
      '../src/cli/goodreads',
      'gr-book-link.parquet',
      '../book-links/cluster-genders.parquet',
    ],
    outs: [
      'gr-work-gender.parquet',
    ],
  },
} + variants[bd.config.goodreads.interactions], bd.config.goodreads.enabled)
