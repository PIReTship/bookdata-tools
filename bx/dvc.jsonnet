local bd = import '../bookdata.libsonnet';

bd.pipeline({
  'clean-ratings': {
    cmd: bd.cmd('bx extract ../data/BX-CSV-Dump.zip cleaned-ratings.csv'),
    deps: [
      '../src/cli/bx',
      '../data/BX-CSV-Dump.zip',
    ],
    outs: [
      'cleaned-ratings.csv',
    ],
  },
  'cluster-ratings': {
    cmd: bd.cmd('bx cluster-actions --ratings -o bx-cluster-ratings.parquet'),
    deps: [
      '../src/cli/bx',
      'cleaned-ratings.csv',
      '../book-links/isbn-clusters.parquet',
    ],
    outs: [
      'bx-cluster-ratings.parquet',
    ],
  },
  'cluster-actions': {
    cmd: bd.cmd('bx cluster-actions --add-actions -o bx-cluster-actions.parquet'),
    deps: [
      '../src/cli/bx',
      'cleaned-ratings.csv',
      '../book-links/isbn-clusters.parquet',
    ],
    outs: [
      'bx-cluster-actions.parquet',
    ],
  },
}, bd.config.bx.enabled)
