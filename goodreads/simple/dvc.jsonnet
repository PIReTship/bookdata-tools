local bd = import '../../lib.jsonnet';
local cfg = bd.config.goodreads;
local enabled = cfg.enabled && (cfg.enabled == 'all' || cfg.interactions == 'simple');

bd.pipeline({
  'scan-interactions': {
    cmd: bd.cmd('goodreads scan interactions --csv --book-map ../../data/goodreads/book_id_map.csv ../../data/goodreads/goodreads_interactions.csv.gz'),
    deps: [
      '../../src/cli/goodreads',
      '../../src/goodreads',
      '../../data/goodreads/book_id_map.csv',
      '../../data/goodreads/goodreads_interactions.csv.gz',
    ],
    outs: [
      'gr-interactions.parquet',
    ],
  },

  'cluster-actions': {
    wdir: '../..',
    cmd: bd.cmd('goodreads cluster-interactions --add-actions --simple -o goodreads/simple/gr-cluster-actions.parquet'),
    deps: [
      'src/cli/goodreads/cluster.rs',
      'goodreads/gr-book-link.parquet',
      'goodreads/simple/gr-interactions.parquet',
    ],
    outs: [
      'goodreads/simple/gr-cluster-actions.parquet',
    ],
  },

  'cluster-ratings': {
    wdir: '../..',
    cmd: bd.cmd('goodreads cluster-interactions --ratings --simple -o goodreads/simple/gr-cluster-ratings.parquet'),
    deps: [
      'src/cli/goodreads/cluster.rs',
      'goodreads/gr-book-link.parquet',
      'goodreads/simple/gr-interactions.parquet',
    ],
    outs: [
      'goodreads/simple/gr-cluster-ratings.parquet',
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

  'work-actions': {
    wdir: '../..',
    cmd: bd.cmd('goodreads cluster-interactions --add-actions --simple --native-works -o goodreads/simple/gr-work-actions.parquet'),
    deps: [
      'src/cli/goodreads/cluster.rs',
      'goodreads/simple/gr-interactions.parquet',
      'goodreads/gr-book-link.parquet',
    ],
    outs: [
      'goodreads/simple/gr-work-actions.parquet',
    ],
  },
  'work-ratings': {
    wdir: '../..',
    cmd: bd.cmd('goodreads cluster-interactions --ratings --simple --native-works -o goodreads/simple/gr-work-ratings.parquet'),
    deps: [
      'src/cli/goodreads/cluster.rs',
      'goodreads/simple/gr-interactions.parquet',
      'goodreads/gr-book-link.parquet',
    ],
    outs: [
      'goodreads/simple/gr-work-ratings.parquet',
    ],
  },

  'work-ratings-5core': {
    cmd: bd.cmd('kcore -o gr-work-ratings-5core.parquet gr-work-ratings.parquet'),
    deps: [
      '../../src/cli/kcore.rs',
      'gr-work-ratings.parquet',
    ],
    outs: [
      'gr-work-ratings-5core.parquet',
    ],
  },

  'work-actions-5core': {
    cmd: bd.cmd('kcore -o gr-work-actions-5core.parquet gr-work-actions.parquet'),
    deps: [
      '../../src/cli/kcore.rs',
      'gr-work-actions.parquet',
    ],
    outs: [
      'gr-work-actions-5core.parquet',
    ],
  },
}, enabled)
