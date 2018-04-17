const fs = require('fs');

const gulp = require('gulp');
const cp = require('child_process');
const miss = require('mississippi');
const Promise = require('bluebird');
const log = require('gulplog');

const args = require('minimist')(process.argv.slice(2));

const olimport = require('./lib/ol-import');
const lkexport = require('./lib/lkexport');

const olDate = args['ol-date'] || '2017-10-01';

exports.importAuthors = () => olimport.authors(olDate);
exports.importWorks = () => olimport.works(olDate);
exports.importEditions = () => olimport.editions(olDate);

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
  return viaf.import('data/viaf-20180401-clusters-marc21.xml.gz');
};

exports.importLOC = function() {
  var loc = require('./lib/loc-import');
  return loc.import('data/LOC/BooksAll.*.gz');
};

exports.indexLOC = function() {
  return new Promise((ok, fail) => {
    let script = fs.createReadStream('loc-index.sql');
    script.on('open', () => {
      let p = cp.spawn('psql', [], {
        stdio: [script, process.stdout, process.stderr]
      });
      p.on('exit', (code, sig) => {
        if (sig) fail(new Error('psql exited with signal ' + sig));
        else if (code) fail(new Error('psql exited with code ' + code));
        else ok();
      });
      p.on('error', fail);
    });
  });
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
