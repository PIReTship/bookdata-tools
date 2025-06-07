local bd = import '../bookdata.libsonnet';

bd.pipeline({
  'scan-books': {
    cmd: bd.cmd('scan-marc --book-mode --glob "../data/loc-books/BooksAll.2016*.xml.gz"'),
    deps: [
      '../src/cli/scan_marc.rs',
      '../src/marc',
      '../data/loc-books',
    ],
    outs: [
      'book-fields.parquet',
      'book-ids.parquet',
      'book-isbns.parquet',
      'book-authors.parquet',
    ],
  },

  'scan-names': {
    cmd: bd.cmd('scan-marc --glob "../data/loc-names/Names.2016*.xml.gz" -o name-fields.parquet'),
    deps: [
      '../src/cli/scan_marc.rs',
      '../src/marc',
      '../data/loc-names',
    ],
    outs: [
      'name-fields.parquet',
    ],
  },

  'book-isbn-ids': {
    wdir: '..',
    cmd: bd.cmd('link-isbn-ids -R rec_id -o loc-mds/book-isbn-ids.parquet loc-mds/book-isbns.parquet'),
    deps: [
      'loc-mds/book-isbns.parquet',
      'book-links/all-isbns.parquet',
    ],
    outs: [
      'loc-mds/book-isbn-ids.parquet',
    ],
  },
})
