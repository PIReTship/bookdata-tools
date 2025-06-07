local bd = import '../bookdata.libsonnet';

bd.pipeline({
  'scan-authors': {
    cmd: bd.cmd('openlib scan-authors ../data/openlib/ol_dump_authors.txt.gz'),
    deps: [
      '../src/cli/openlib.rs',
      '../src/openlib/',
      '../data/openlib/ol_dump_authors.txt.gz',
    ],
    outs: [
      'authors.parquet',
      'author-names.parquet',
    ],
  },
  'scan-works': {
    cmd: bd.cmd('openlib scan-works ../data/openlib/ol_dump_works.txt.gz'),
    deps: [
      '../src/cli/openlib.rs',
      '../src/openlib/',
      '../data/openlib/ol_dump_works.txt.gz',
    ],
    outs: [
      'works.parquet',
      'work-authors.parquet',
      'work-subjects.parquet',
    ],
  },
  'scan-editions': {
    cmd: bd.cmd('openlib scan-editions ../data/openlib/ol_dump_editions.txt.gz'),
    deps: [
      '../src/cli/openlib.rs',
      '../src/openlib/',
      '../data/openlib/ol_dump_editions.txt.gz',
    ],
    outs: [
      'editions.parquet',
      'edition-works.parquet',
      'edition-isbns.parquet',
      'edition-authors.parquet',
      'edition-subjects.parquet',
    ],
  },
  'edition-isbn-ids': {
    wdir: '..',
    cmd: bd.cmd('link-isbn-ids -R edition -o openlibrary/edition-isbn-ids.parquet openlibrary/edition-isbns.parquet'),
    deps: [
      'openlibrary/edition-isbns.parquet',
      'book-links/all-isbns.parquet',
    ],
    outs: [
      'openlibrary/edition-isbn-ids.parquet',
    ],
  },
  'work-clusters': {
    wdir: '..',
    cmd: bd.cmd('cluster extract-books -n work_id -o openlibrary/work-clusters.parquet OL-W'),
    deps: [
      'book-links/cluster-graph-nodes.parquet',
    ],
    outs: [
      'openlibrary/work-clusters.parquet',
    ],
  },
})
