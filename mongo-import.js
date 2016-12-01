"use strict";

const zlib = require('zlib');
const util = require('util');
const async = require('async');
const io = require('./lib/io');
const MongoClient = require('mongodb').MongoClient;
const miss = require('mississippi');

const options = require('yargs').argv;

const m = {
  host: options.host || 'localhost',
  port: options.port || '27017',
  db: options.db || 'ol'
};
const mongoURI = util.format("mongodb://%s:%s/%s", m.host, m.port, m.db);

const date = options.date || '2016-07-31';

MongoClient.connect(mongoURI, (err, db) => {
  if (err) throw err;
  console.info('connected to %s', mongoURI);

  async.series([
    (cb) => importFile(db.collection('authors'), 'authors', cb),
    (cb) => importFile(db.collection('works'), 'works', cb),
    (cb) => importFile(db.collection('editions'), 'editions', cb)
  ], (err) => {
    db.close();
    if (err) throw err;
  });
});

function importFile(collection, name, done) {
  var path = util.format('data/ol_dump_%s_%s.txt.gz', name, date);
  console.info('loading %s', path);
  io.openFile(path, (err, stream) => {
    if (err) return done(err);

    var dest = miss.to.obj((data, enc, cb) => {
      collection.insert(data, {w: 0}, (err, res) => {
        cb(err);
      });
    }, (cb) => {
      cb();
      done();
    });
    stream.pipe(zlib.createUnzip())
          .pipe(io.decodeLines())
          .pipe(dest);
  });
}