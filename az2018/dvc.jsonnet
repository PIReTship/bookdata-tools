local bd = import '../lib.jsonnet';

bd.pipeline({
  'scan-ratings': {
    cmd: bd.cmd('amazon scan-ratings -o ratings.parquet --swap-id-columns ../data/az2018/Books.csv'),
    deps: [
      '../src/amazon.rs',
      '../src/cli/amazon/',
      '../data/az2018/Books.csv',
    ],
    outs: ['ratings.parquet'],
  },

  'cluster-ratings': {
    wdir: '..',
    cmd: bd.cmd('amazon cluster-ratings -o az2018/az-cluster-ratings.parquet az2018/ratings.parquet'),
    deps: [
      'src/cli/amazon',
      'az2018/ratings.parquet',
      'book-links/isbn-clusters.parquet',
    ],
    outs: ['az2018/az-cluster-ratings.parquet'],
  },

  'cluster-ratings-5core': {
    cmd: bd.cmd('kcore -o az-cluster-ratings-5core.parquet az-cluster-ratings.parquet'),
    deps: [
      'az-cluster-ratings.parquet',
      '../src/cli/kcore.rs',
    ],
    outs: ['az-cluster-ratings-5core.parquet'],
  },
})
