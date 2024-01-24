local bd = import '../lib.jsonnet';

{
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

  'scan-interactions': {
    cmd: bd.cmd('goodreads scan interactions ../data/goodreads/goodreads_interactions.json.gz'),
    deps: [
      '../src/cli/goodreads',
      '../src/goodreads',
      '../data/goodreads/goodreads_interactions.json.gz',
    ],
    outs: [
      'gr-interactions.parquet',
    ],
  },

} + if bd.config.goodreads.reviews then {
  'scan-reviews': {
    cmd: bd.cmd('goodreads scan reviews ../data/goodreads/goodreads_reviews_dedup.json.gz'),
    deps: [
      '../src/cli/goodreads',
      '../src/goodreads',
      '../data/goodreads/goodreads_reviews_dedup.json.gz',
      'gr-book-link.parquet',
      'gr-users.parquet',
    ],
    outs: [
      'gr-reviews.parquet',
    ],
  },
} else {}
