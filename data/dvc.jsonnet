local bd = import '../lib.jsonnet';

local loc = {
  dl_base: 'https://www.loc.gov/cds/downloads/MDSConnect/',
  book_base: 'BooksAll.2016.part',
  book_range: '01-43',
  name_base: 'Names.2016.part',
  name_range: '01-40',
};

local olUrl = function(part, date)
  std.format('https://openlibrary.org/data/ol_dump_%s_%s.txt.gz', [part, date]);
local viafUrl = function(date)
  std.format('https://viaf.org/viaf/data/viaf-%s-clusters-marc21.xml.gz', [std.strReplace(date, '-', '')]);

local mdsCurl = function(folder, base, range) {
  local url = std.format('%s%s[%s].xml.gz', [loc.dl_base, base, range]),
  local out = std.format('%s/%s#1.xml.gz', [folder, base]),

  cmd: std.format('curl -fsSL %s -o %s --create-dirs', [url, out]),
  outs: [folder],
};

local curl = function(url, file) {
  cmd: std.format('curl -fsSL --retry 100 -o %s %s', [file, url]),
  outs: [file],
};

bd.pipeline({
  'loc-books': mdsCurl('loc-books', loc.book_base, loc.book_range),
  'loc-names': mdsCurl('loc-names', loc.name_base, loc.name_range),

  'viaf-clusters': curl(viafUrl(bd.config.viaf.date), 'viaf-clusters-marc21.xml.gz'),

  'ol-editions': curl(olUrl('editions', bd.config.openlibrary.date), 'openlib/ol_dump_editions.txt.gz'),
  'ol-authors': curl(olUrl('authors', bd.config.openlibrary.date), 'openlib/ol_dump_authors.txt.gz'),
  'ol-works': curl(olUrl('works', bd.config.openlibrary.date), 'openlib/ol_dump_works.txt.gz'),
})
