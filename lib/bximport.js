const fs = require('fs');
const csv = require('csv-parser');
const through = require('through2');

const ioUtils = require('./io');
const QueryEvalStream = require('./query-eval-stream');

function importBXRatings(fn) {
  return ioUtils.openFile(fn)
           .pipe(ioUtils.decodeBadUnicode())
           .pipe(csv({
             separator: ';',
             escape: '\\'
           }))
           .pipe(through.obj((row, enc, cb) => {
             cb(null, {
               text: 'INSERT INTO bx_ratings (user_id, isbn, rating) VALUES ($1, $2, $3)',
               name: 'insert-rating',
               values: [row['User-ID'], row['ISBN'].replace(/"/, ''), row['Book-Rating']]
             });
           }))
           .pipe(new QueryEvalStream());
}

module.exports = importBXRatings;
