const fs = require('fs-extra');
const zlib = require('zlib');
const cp = require('child_process');

function importTable(key, file) {
  let s = fs.createReadStream(file);
  let p = cp.spawn('psql', ['-c', `\\copy gr_raw_${key} FROM STDIN`], {
    stdio: ['pipe', process.stdout, process.stderr]
  });
  s.pipe(zlib.createGunzip()).pipe(p.stdin);
  return p;
}

exports.importBooks = function() {
  return importTable('book', 'data/goodreads_books.json.gz');
}
exports.importInteractions = function() {
  return importTable('interactions', 'data/goodreads_interactions.json.gz');
}
exports.importWorks = function() {
  return importTable('work', 'data/goodreads_book_works.json.gz');
}
exports.importAuthors = function() {
  return importTable('author', 'data/goodreads_book_authors.json.gz');
}
