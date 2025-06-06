local bd = import '../bookdata.libsonnet';

bd.pipeline({
  'scan-ratings': {
    cmd: bd.cmd('amazon scan-ratings -o ratings.parquet ../data/az2014/ratings_Books.csv'),
    deps: [
      '../src/amazon.rs',
      '../src/cli/amazon/',
      '../data/az2014/ratings_Books.csv',
    ],
    outs: ['ratings.parquet'],
  },

  'cluster-ratings': {
    wdir: '..',
    cmd: bd.cmd('amazon cluster-ratings -o az2014/az-cluster-ratings.parquet az2014/ratings.parquet'),
    deps: [
      'src/cli/amazon',
      'az2014/ratings.parquet',
      'book-links/isbn-clusters.parquet',
    ],
    outs: ['az2014/az-cluster-ratings.parquet'],
  },

  'cluster-ratings-5core': {
    cmd: bd.cmd('kcore -o az-cluster-ratings-5core.parquet az-cluster-ratings.parquet'),
    deps: [
      'az-cluster-ratings.parquet',
      '../src/cli/kcore.rs',
    ],
    outs: ['az-cluster-ratings-5core.parquet'],
  },
}, bd.config.az2014.enabled)
