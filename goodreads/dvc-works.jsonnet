local bd = import '../lib.jsonnet';

{
  'work-item-info':{
    cmd: 'python gr-work-items.py',
    deps: [
      'gr-work-items.py',
      'gr-book-ids.parquet',
      'gr-book-info.parquet',
      'gr-work-info.parquet',
    ],
    outs: [
      'gr-work-item-info.parquet',
    ]
  },
  'work-actions': {
    wdir: '..',
    cmd: bd.cmd('goodreads cluster-interactions --add-actions --native-works -o goodreads/gr-work-actions.parquet'),
    deps: [
      'src/cli/goodreads/cluster.rs',
      'goodreads/gr-book-link.parquet',
      'goodreads/gr-interactions.parquet',
    ],
    outs: [
      'goodreads/gr-work-actions.parquet',
    ],
  },

  'work-ratings': {
    wdir: '..',
    cmd: bd.cmd('goodreads cluster-interactions --ratings --native-works -o goodreads/gr-work-ratings.parquet'),
    deps: [
      'src/cli/goodreads/cluster.rs',
      'goodreads/gr-book-link.parquet',
      'goodreads/gr-interactions.parquet',
    ],
    outs: [
      'goodreads/gr-work-ratings.parquet',
    ],
  },

  'work-ratings-5core': {
    cmd: bd.cmd('kcore -o gr-work-ratings-5core.parquet gr-work-ratings.parquet'),
    deps: [
      '../src/cli/kcore.rs',
      'gr-work-ratings.parquet',
    ],
    outs: [
      'gr-work-ratings-5core.parquet',
    ],
  },

  'work-actions-5core': {
    cmd: bd.cmd('kcore -o gr-work-actions-5core.parquet gr-work-actions.parquet'),
    deps: [
      '../src/cli/kcore.rs',
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
      '../src/cli/kcore.rs',
    ],
    outs: [
      'gr-work-ratings-2015-100-10core.parquet',
    ],
  },

  'work-actions-2015-100-10core': {
    cmd: bd.cmd('kcore --user-k 10 --item-k 100 --year 2015 -o gr-work-actions-2015-100-10core.parquet gr-work-actions.parquet'),
    deps: [
      'gr-work-actions.parquet',
      '../src/cli/kcore.rs',
    ],
    outs: [
      'gr-work-actions-2015-100-10core.parquet',
    ],
  },

  'work-gender': {
    cmd: bd.cmd('goodreads work-gender'),
    deps: [
      '../src/cli/goodreads',
      'gr-book-link.parquet',
      '../book-links/cluster-genders.parquet',
    ],
    outs: [
      'gr-work-gender.parquet',
    ],
  },
}
