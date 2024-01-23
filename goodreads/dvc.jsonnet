local bd = import '../lib.jsonnet';

local clusters = import 'dvc-clusters.jsonnet';
local scan = import 'dvc-scan.jsonnet';
local works = import 'dvc-works.jsonnet';
local links = {
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
};

bd.pipeline(scan + links + clusters + works, bd.config.goodreads.enabled)
