local bd = import '../bookdata.libsonnet';

bd.pipeline({
  'scan-authors': {
    cmd: bd.cmd('scan-marc -L -o viaf.parquet ../data/viaf-clusters-marc21.xml.gz'),
    deps: [
      '../src/cli/scan_marc.rs',
      '../src/marc',
      '../data/viaf-clusters-marc21.xml.gz',
    ],
    outs: [
      'viaf.parquet',
    ],
  },
  'author-genders': {
    cmd: bd.cmd('filter-marc --tag=375 --subfield=a --trim --lower -n gender -o author-genders.parquet viaf.parquet'),
    deps: [
      '../src/cli/filter_marc.rs',
      'viaf.parquet',
    ],
    outs: [
      'author-genders.parquet',
    ],
  },
  'index-names': {
    cmd: bd.cmd('index-names --marc-authorities viaf.parquet author-name-index.parquet'),
    deps: [
      '../src/cli/index_names.rs',
      '../src/cleaning/names',
      'viaf.parquet',
    ],
    outs: [
      'author-name-index.parquet',
      'author-name-index.csv.gz',
    ],
  },
})
