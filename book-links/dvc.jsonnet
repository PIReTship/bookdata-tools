local bd = import '../bookdata.libsonnet';

bd.pipeline({
  'collect-isbns': {
    cmd: bd.cmd('collect-isbns -o all-isbns.parquet'),
    deps: std.prune([
      '../config.yaml',
      '../src/cli/collect_isbns.rs',
      '../loc-mds/book-isbns.parquet',
      '../openlibrary/edition-isbns.parquet',
      bd.maybe(bd.config.goodreads.enabled, '../goodreads/gr-book-ids.parquet'),
      bd.maybe(bd.config.bx.enabled, '../bx/cleaned-ratings.csv'),
      bd.maybe(bd.config.az2014.enabled, '../az2014/ratings.parquet'),
      bd.maybe(bd.config.az2018.enabled, '../az2018/ratings.parquet'),
    ]),
    outs: [
      'all-isbns.parquet',
    ],
  },

  cluster: {
    wdir: '..',
    cmd: bd.cmd('cluster-books --save-graph book-links/book-graph.mp.zst'),
    deps: [
      'src/cli/cluster_books.rs',
      'src/graph/',
      'book-links/all-isbns.parquet',
      'loc-mds/book-ids.parquet',
      'loc-mds/book-isbn-ids.parquet',
      'openlibrary/editions.parquet',
      'openlibrary/works.parquet',
      'openlibrary/edition-isbn-ids.parquet',
      'openlibrary/edition-works.parquet',
    ] + if bd.config.goodreads.enabled then [
      'goodreads/gr-book-ids.parquet',
      'goodreads/book-isbn-ids.parquet',
    ] else [],
    outs: [
      'book-links/book-graph.mp.zst',
      'book-links/isbn-clusters.parquet',
      'book-links/cluster-stats.parquet',
      'book-links/cluster-graph-nodes.parquet',
      'book-links/cluster-graph-edges.parquet',
    ],
    metrics: [
      { 'book-links/cluster-metrics.json': { cache: false } },
    ],
  },

  'cluster-ol-first-authors': {
    wdir: '..',
    cmd: bd.cmd('cluster extract-authors -o book-links/cluster-ol-first-authors.parquet --first-author -s openlib'),
    deps: [
      'src/cli/cluster',
      'book-links/isbn-clusters.parquet',
      'openlibrary/edition-isbn-ids.parquet',
      'openlibrary/edition-authors.parquet',
      'openlibrary/author-names.parquet',
    ],
    outs: [
      'book-links/cluster-ol-first-authors.parquet',
    ],
  },

  'cluster-loc-first-authors': {
    wdir: '..',
    cmd: bd.cmd('cluster extract-authors -o book-links/cluster-loc-first-authors.parquet --first-author -s loc'),
    deps: [
      'src/cli/cluster',
      'book-links/isbn-clusters.parquet',
      'loc-mds/book-isbn-ids.parquet',
      'loc-mds/book-authors.parquet',
    ],
    outs: [
      'book-links/cluster-loc-first-authors.parquet',
    ],
  },

  'cluster-first-authors': {
    wdir: '..',
    cmd: bd.cmd('cluster extract-authors -o book-links/cluster-first-authors.parquet --first-author -s openlib -s loc'),
    deps: [
      'src/cli/cluster',
      'book-links/isbn-clusters.parquet',
      'openlibrary/edition-isbn-ids.parquet',
      'openlibrary/edition-authors.parquet',
      'openlibrary/author-names.parquet',
      'loc-mds/book-isbn-ids.parquet',
      'loc-mds/book-authors.parquet',
    ],
    outs: [
      'book-links/cluster-first-authors.parquet',
    ],
  },

  'cluster-genders': {
    wdir: '..',
    cmd: bd.cmd('cluster extract-author-gender -o book-links/cluster-genders.parquet -A book-links/cluster-first-authors.parquet'),
    deps: [
      'src/cli/cluster',
      'book-links/cluster-stats.parquet',
      'book-links/cluster-first-authors.parquet',
      'viaf/author-name-index.parquet',
      'viaf/author-genders.parquet',
    ],
    outs: [
      'book-links/cluster-genders.parquet',
    ],
  },

  'gender-stats': {
    wdir: '..',
    cmd: bd.cmd('integration-stats'),
    deps: std.prune([
      'src/cli/stats.rs',
      'book-links/cluster-genders.parquet',
      'book-links/isbn-clusters.parquet',
      'loc-mds/book-isbn-ids.parquet',
      bd.maybe(bd.config.bx.enabled, 'bx/bx-cluster-actions.parquet'),
      bd.maybe(bd.config.bx.enabled, 'bx/bx-cluster-ratings.parquet'),
      bd.maybe(bd.config.az2014.enabled, 'az2014/az-cluster-ratings.parquet'),
      bd.maybe(bd.config.az2018.enabled, 'az2018/az-cluster-ratings.parquet'),
      bd.maybe(bd.config.goodreads.enabled, 'goodreads/gr-cluster-actions.parquet'),
      bd.maybe(bd.config.goodreads.enabled, 'goodreads/gr-cluster-ratings.parquet'),
    ]),
    outs: [
      'book-links/gender-stats.csv',
    ],
  },

  'cluster-hashes': {
    cmd: bd.cmd('cluster hash -o cluster-hashes.parquet isbn-clusters.parquet'),
    deps: [
      '../src/cli/cluster/hash.rs',
      'isbn-clusters.parquet',
    ],
    outs: [
      'cluster-hashes.parquet',
    ],
  },
})
