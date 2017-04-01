const fs = require('fs');

const gulp = require('gulp');
const gutil = require('gulp-util');
const cp = require('child_process');

const pgimport = require('./pgimport');
const lkexport = require('./lib/lkexport');

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
};

exports.exportAmazon = lkexport.amazon;
exports.exportBXExplicit = lkexport.bxExplicit;
exports.exportBXAll = lkexport.bxAll;
exports.export = gulp.parallel(
  (cb) => fs.mkdir('out', cb),
  () => lkexport.amazon('out/amazon.csv'),
  () => lkexport.bxAll('out/bx-all.csv'),
  () => lkexport.bxExplicit('out/bx-explicit.csv')
);
