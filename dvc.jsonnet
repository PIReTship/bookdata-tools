local bd = import './lib.jsonnet';

local subpipes = {
  'loc-mds': import 'loc-mds/dvc.jsonnet',
  openlibrary: import 'openlibrary/dvc.jsonnet',
  viaf: import 'viaf/dvc.jsonnet',

  az2014: import 'az2014/dvc.jsonnet',
  az2018: import 'az2018/dvc.jsonnet',
  bx: import 'bx/dvc.jsonnet',
  goodreads: import 'goodreads/dvc.jsonnet',

  'book-links': import 'book-links/dvc.jsonnet',
};
local parquets = [
  std.strReplace(out, '.parquet', '')
  for dir in std.objectFields(subpipes)
  for stage in std.objectValues(subpipes[dir].stages)
  for out in bd.stageOuts(dir, stage)
  if std.endsWith(out, '.parquet')
];

bd.pipeline({
  ClusterStats: {
    cmd: 'quarto render ClusterStats.qmd',
    deps: [
      'ClusterStats.qmd',
      'book-links/cluster-stats.parquet',
    ],
    outs: [
      { 'ClusterStats.ipynb': { cache: false } },
      'ClusterStats.html',
      'ClusterStats_files',
    ],
  },

  LinkageStats: {
    cmd: 'quarto render LinkageStats.qmd',
    deps: [
      'LinkageStats.qmd',
      'book-links/gender-stats.csv',
    ],
    outs: [
      { 'LinkageStats.ipynb': { cache: false } },
      'LinkageStats.html',
      'LinkageStats_files',
    ],
    metrics: [
      'book-coverage.json',
    ],
  },

  schema: {
    foreach: parquets,
    do: {
      cmd: bd.cmd('pq-info -o ${item}.json ${item}.parquet'),
      deps: ['${item}.parquet'],
      outs: [
        { '${item}.json': { cache: false } },
      ],
    },
  },
})
