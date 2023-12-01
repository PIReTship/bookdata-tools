local bd = import './lib.jsonnet';

local subpipes = {
  'loc-mds': import 'loc-mds/dvc.jsonnet',
  openlibrary: import 'openlibrary/dvc.jsonnet',
  viaf: import 'viaf/dvc.jsonnet',

  az2014: import 'az2014/dvc.jsonnet',
  az2018: import 'az2018/dvc.jsonnet',
  bx: import 'bx/dvc.jsonnet',
  goodreads: import 'goodreads/dvc.jsonnet',
  'goodreads/simple': import 'goodreads/simple/dvc.jsonnet',
  'goodreads/full': import 'goodreads/full/dvc.jsonnet',

  'book-links': import 'book-links/dvc.jsonnet',
};
local parquets = [
  std.strReplace(out, '.parquet', '')
  for dir in std.objectFields(subpipes)
  for stage in std.objectValues(subpipes[dir].stages)
  for out in bd.stageOuts(dir, stage)
  if std.endsWith(out, '.parquet')
];

local notebook = function(name, deps=[]) {
  cmd: std.format('quarto render %s.qmd --to html', name),
  deps: [
    name + '.qmd',
  ] + deps,
  outs: [
    { [name + '.ipynb']: { cache: false } },
    name + '.html',
    name + '_files',
  ],
};

bd.pipeline({
  ClusterStats: notebook('ClusterStats', ['book-links/cluster-stats.parquet']),

  LinkageStats: notebook('LinkageStats', [
    'book-links/gender-stats.csv',
  ]),

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
