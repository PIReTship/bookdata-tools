"use strict";

const util = require('util');
const zlib = require('zlib');
const fs = require('fs');
const through = require('through2');
const fws = require('flush-write-stream');
const pg = require('pg');
const async = require('async');
const throughput = require('./lib/throughput');
const io = require('./lib/io');

const options = require('yargs').argv;
const date = options.date || '2016-07-31';

var ninserts = 0;

var autp = throughput('authors');
var wtp = throughput('works');
var etp = throughput('editions');

/**
 * Pipe that runs PostgreSQL queries.
 */
function runQueries(client, finished) {
  var nqueries = 0;
  var started = false;

  function write(data, enc, next) {
    async.series([
      (cb) => {
        if (started) {
          cb();
        } else {
          console.info('starting');
          client.query('BEGIN ISOLATION LEVEL READ UNCOMMITTED', (err) => {
            started = true;
            console.info('started transaction');
            cb(err);
          });
        }
      },
      (cb) => client.query(data, cb),
      (cb) => {
        nqueries += 1;
        if (nqueries % 10000 === 0) {
          console.info('committing');
          async.series([
            (cb) => client.query('COMMIT', cb),
            (cb) => client.query('BEGIN ISOLATION LEVEL READ UNCOMMITTED', cb)
          ], cb);
        } else {
          process.nextTick(cb);
        }
      }
    ], next);
  }

  function flush(cb) {
    client.query('COMMIT', (err) => {
      process.nextTick(finished, err, nqueries);
    });
  }

  return fws.obj(write, flush);
}

const imports = {
  authors: function (rec) {
    return {
      text: 'INSERT INTO authors (author_key, author_name, author_data) VALUES ($1, $2, $3)',
      name: 'insert-author',
      values: [rec.key, rec.name, JSON.stringify(rec)]
    };
  }, 
  works: function (rec) {
    return {
      text: 'INSERT INTO works (work_key, work_title, work_data) VALUES ($1, $2, $3)',
      name: 'insert-work',
      values: [rec.key, rec.title, JSON.stringify(rec)]
    };               
  },
  editions: function(rec) {
    return {
      text: 'INSERT INTO editions (edition_key, edition_title, edition_data) VALUES ($1, $2, $3)',
      name: 'insert-edition',
      values: [rec.key, rec.title, JSON.stringify(rec)]
    };
  }
};

function doImport(name, callback) {
  const proc = imports[name];
  if (proc === undefined) {
    return callback(new Error("no such import " + name));
  }
  const client = new pg.Client(options['db-url']);

  async.waterfall([
    client.connect.bind(client),
    (_, cb) => io.openFile(util.format("data/ol_dump_%s_%s.txt.gz", name, date), cb),
    (stream, cb) => {
      stream.pipe(zlib.createUnzip())
            .pipe(io.decodeLines())
            .pipe(through.obj((rec, enc, cb) => {
              cb(null, proc(rec));
            }))
            .pipe(runQueries(client, cb));
    }
  ], (err) => {
    if (err) {
      console.error("error running %s: %s", name, err);
    } else {
      console.info("finished %s", name);
    }
    client.end((e2) => {
      if (err) {
        callback(err);
      } else if (e2) {
        callback(e2);
      } else {
        callback();
      }
    });
  });
}

async.parallel(options._.map((n) => (cb) => doImport(n, cb)), (err) => {
  if (err) {
    throw err;
  }
});