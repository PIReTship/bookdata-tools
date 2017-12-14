const fs = require('fs');
const zlib = require('zlib');
const pg = require('pg');
const QueryStream = require('pg-query-stream');
const Q = require('q');
const csv = require('fast-csv');

const queries = {
  amazon: 'SELECT user_id, book_id, median(rating) FROM az_ratings JOIN az_users USING (user_key) JOIN isbn_book_id ON (asin = isbn) GROUP BY user_id, book_id',
  'bx-explicit': 'SELECT user_id, book_id, median(rating) AS rating FROM bx_ratings JOIN isbn_book_id USING (isbn) WHERE rating > 0 GROUP BY user_id, book_id',
  'bx-all': 'SELECT user_id, book_id, rating FROM bx_ratings JOIN isbn_book_id USING (isbn)'
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
    qstr.pipe(csv.createWriteStream({headers: true})
                 .transform((row) => {
                   return {
                     userID: row.user_id,
                     bookID: row.book_id,
                     rating: row.rating
                   };
                 }))
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
