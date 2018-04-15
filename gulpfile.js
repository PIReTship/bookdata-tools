const fs = require('fs');

const gulp = require('gulp');
const cp = require('child_process');
const miss = require('mississippi');

const args = require('minimist')(process.argv.slice(2));

const olimport = require('./lib/ol-import');
const lkexport = require('./lib/lkexport');

exports.importAuthors = olimport.authors;
exports.importWorks = olimport.works;
exports.importEditions = olimport.editions;

exports.importOpenLib = gulp.parallel(
  exports.importAuthors,
  exports.importWorks,
  exports.importEditions
);
exports.importOpenLib.description = 'Import all OpenLib data';

exports.importAmazon = function() {
  return cp.spawn('psql', ['-c', "\\copy az_ratings FROM 'data/ratings_Books.csv' WITH CSV"], {
    stdio: ['ignore', process.stdout, process.stderr]
  });
};

exports.importBX = function() {
  const bxi = require('./lib/bximport');
  return bxi('data/BX-Book-Ratings.csv');
};

exports.importVIAF = function() {
  var viaf = require('./lib/viaf-import');
  return viaf.import('data/viaf/viaf-20171106-clusters-marc21.xml.gz', args['db-url']);
};

exports.importLOC = function() {
  var loc = require('./lib/loc-import');
  return gulp.src('data/LOC/BooksAll.*.gz', {read: false})
             .pipe(miss.to.obj((file, enc, cb) => {
               loc.import(file.path, args['db-url']).then(() => cb(), cb);
             }));
};

exports.export = gulp.series(
  function mkdir(cb) {
    fs.mkdir('out', (err) => {
      if (err && err.code != 'EEXIST') {
        cb(err);
      } else {
        cb();
      }
    })
  },
  gulp.parallel(
    function amazon() { return lkexport.amazon('out/az-ratings.csv') },
    function bxAll() { return lkexport.bxAll('out/bx-implicit.csv') },
    function bxExplicit() { return lkexport.bxExplicit('out/bx-ratings.csv') }
  ));
