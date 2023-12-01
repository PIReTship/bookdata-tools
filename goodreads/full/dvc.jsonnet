local bd = import '../../lib.jsonnet';
local cfg = bd.config.goodreads;
local enabled = cfg.enabled && (cfg.build_all || cfg.interactions == 'full');

bd.pipeline({
  'scan-interactions': {
    cmd: bd.cmd('goodreads scan interactions ../../data/goodreads/goodreads_interactions.json.gz'),
    deps: [
      '../../src/cli/goodreads',
      '../../src/goodreads',
      '../../data/goodreads/goodreads_interactions.json.gz',
    ],
    outs: [
      'gr-interactions.parquet',
      'gr-users.parquet',
    ],
  },

  'cluster-actions': {
    wdir: '../..',
    cmd: bd.cmd('goodreads cluster-interactions --add-actions -o goodreads/full/gr-cluster-actions.parquet'),
    deps: [
      'src/cli/goodreads/cluster.rs',
      'goodreads/gr-book-link.parquet',
      'goodreads/full/gr-interactions.parquet',
    ],
    outs: [
      'goodreads/full/gr-cluster-actions.parquet',
    ],
  },

  'cluster-ratings': {
    wdir: '..',
    cmd: bd.cmd('goodreads cluster-interactions --ratings -o goodreads/full/gr-cluster-ratings.parquet'),
    deps: [
      'src/cli/goodreads/cluster.rs',
      'goodreads/gr-book-link.parquet',
      'goodreads/full/gr-interactions.parquet',
    ],
    outs: [
      'goodreads/full/gr-cluster-ratings.parquet',
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
    wdir: '..',
    cmd: bd.cmd('goodreads cluster-interactions --add-actions --native-works -o goodreads/full/gr-work-actions.parquet'),
    deps: [
      'src/cli/goodreads/cluster.rs',
      'goodreads/gr-book-link.parquet',
      'goodreads/full/gr-interactions.parquet',
    ],
    outs: [
      'goodreads/full/gr-work-actions.parquet',
    ],
  },

  'work-ratings': {
    wdir: '..',
    cmd: bd.cmd('goodreads cluster-interactions --ratings --native-works -o goodreads/full/gr-work-ratings.parquet'),
    deps: [
      'src/cli/goodreads/cluster.rs',
      'goodreads/gr-book-link.parquet',
      'goodreads/full/gr-interactions.parquet',
    ],
    outs: [
      'goodreads/full/gr-work-ratings.parquet',
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

  'work-ratings-2015-100-10core': {
    cmd: bd.cmd('kcore --user-k 10 --item-k 100 --year 2015 -o gr-work-ratings-2015-100-10core.parquet gr-work-ratings.parquet'),
    deps: [
      'gr-work-ratings.parquet',
      '../../src/cli/kcore.rs',
    ],
    outs: [
      'gr-work-ratings-2015-100-10core.parquet',
    ],
  },

  'work-actions-2015-100-10core': {
    cmd: bd.cmd('kcore --user-k 10 --item-k 100 --year 2015 -o gr-work-actions-2015-100-10core.parquet gr-work-actions.parquet'),
    deps: [
      'gr-work-actions.parquet',
      '../../src/cli/kcore.rs',
    ],
    outs: [
      'gr-work-actions-2015-100-10core.parquet',
    ],
  },
}, enabled)
