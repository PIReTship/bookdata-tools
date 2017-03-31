const gulp = require('gulp');
const gutil = require('gulp-util');
const cp = require('child_process');

const pgimport = require('./pgimport');

exports.importAuthors = pgimport.authors;
exports.importWorks = pgimport.works;
exports.importEditions = pgimport.editions;

exports.importOpenLib = gulp.parallel(
  exports.importAuthors,
  exports.importWorks,
  exports.importEditions
);
exports.importOpenLib.description = 'Import all OpenLib data';

exports.importAmazon = function() {
  return cp.spawn(psql, ['-c', "\\copy az_ratings FROM 'data/ratings_Books.csv' WITH CSV"], {
    stdio: ['ignore', process.stdout, process.stderr]
  });
};

exports.importBX = function() {
  const bxi = require('./lib/bximport');
  return bxi('data/BX-Book-Ratings.csv');
}
