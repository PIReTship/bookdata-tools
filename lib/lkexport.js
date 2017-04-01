const fs = require('fs');
const zlib = require('zlib');
const pg = require('pg');
const QueryStream = require('pg-query-stream');
const Q = require('q');
const csv = require('fast-csv');

const queries = {
  amazon: 'SELECT user_id AS userID, book_id AS bookID, rating FROM az_ratings JOIN az_users USING (user_key) JOIN isbn_book_id ON (asin = isbn)',
  'bx-explicit': 'SELECT user_id AS userID, book_id AS bookID, rating FROM bx_ratings JOIN isbn_book_id USING (isbn) WHERE ratin > 0',
  'bx-all': 'SELECT user_id AS userID, book_id AS bookID, rating FROM bx_ratings JOIN isbn_book_id USING (isbn)'
};

function exportRatingTriples(type, outFile) {
  let rq = queries[type];
  if (!rq) {
    return Q.reject(`invalid data type ${type}`);
  }

  var out = fs.createWriteStream(outFile, 'utf8');
  var query = new QueryStream(rq);

  pg.connect(null, (err, client, done) => {
    if (err) {
      out.emit('error', err);
      out.close();
      throw err;
    }
    var qstr = client.query(query);
    qstr.pipe(csv.createWriteStream({headers: true}))
        .pipe(zlib.createGzip())
        .pipe(out)
        .on('error', done)
        .on('finish', () => done());
  });

  return out;
}

module.exports = {
  amazon: exportRatingTriples.bind(null, 'amazon'),
  bxExplicit: exportRatingTriples.bind(null, 'bx-explicit'),
  bxAll: exportRatingTriples.bind(null, 'bx-all')
};
