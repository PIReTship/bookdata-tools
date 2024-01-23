local bd = import '../lib.jsonnet';

{
  'cluster-actions': {
    wdir: '../..',
    cmd: bd.cmd('goodreads cluster-interactions --add-actions -o goodreads/gr-cluster-actions.parquet'),
    deps: [
      'src/cli/goodreads/cluster.rs',
      'goodreads/gr-book-link.parquet',
      'goodreads/gr-interactions.parquet',
    ],
    outs: [
      'goodreads/gr-cluster-actions.parquet',
    ],
  },

  'cluster-ratings': {
    wdir: '../..',
    cmd: bd.cmd('goodreads cluster-interactions --ratings -o goodreads/gr-cluster-ratings.parquet'),
    deps: [
      'src/cli/goodreads/cluster.rs',
      'goodreads/gr-book-link.parquet',
      'goodreads/gr-interactions.parquet',
    ],
    outs: [
      'goodreads/gr-cluster-ratings.parquet',
    ],
  },

  'cluster-ratings-5core': {
    cmd: bd.cmd('kcore -o gr-cluster-ratings-5core.parquet gr-cluster-ratings.parquet'),
    deps: [
      '../../src/cli/kcore.rs',
      'gr-cluster-ratings.parquet',
    ],
    outs: [
      'gr-cluster-ratings-5core.parquet',
    ],
  },

  'cluster-actions-5core': {
    cmd: bd.cmd('kcore -o gr-cluster-actions-5core.parquet gr-cluster-actions.parquet'),
    deps: [
      '../../src/cli/kcore.rs',
      'gr-cluster-actions.parquet',
    ],
    outs: [
      'gr-cluster-actions-5core.parquet',
    ],
  },
}
