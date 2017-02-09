"use strict";
const miss = require('mississippi');
const async = require('async');

function runQueries(db, options) {
  if (options === undefined) {
    options = {};
  }
  var batchSize = options.batchSize;
  if (batchSize === undefined) {
    batchSize = 10000;
  }
  var nqueries = 0;
  var started = false;
  var manage = !db || typeof(db) === 'string';
  var client;

  if (!manage) {
    client = db;
  }

  function write(data, enc, next) {
    async.series([
      (cb) => {
        if (client) {
          cb();
        } else {
          var pg = require('pg');
          if (options.native) {
            pg = pg.native;
          }
          client = new pg.Client(db);
          client.connect((err) => cb(err));
        }
      },
      (cb) => {
        if (started || !batchSize) {
          cb();
        } else {
          client.query('BEGIN ISOLATION LEVEL READ UNCOMMITTED', (err) => {
            started = true;
            cb(err);
          });
        }
      },
      (cb) => client.query(data, cb),
      (cb) => {
        nqueries += 1;
        if (batchSize && nqueries % batchSize === 0) {
          if (options.logger) {
            options.logger.debug('committing');
          }
          async.series([
            (cb) => client.query('COMMIT', cb),
            (cb) => client.query('BEGIN ISOLATION LEVEL READ UNCOMMITTED', cb)
          ], cb);
        } else {
          process.nextTick(cb);
        }
      }
    ], (err) => {
      if (options.logger) {
        options.logger.error('error in query %s: %s', data.name, data.message);
        options.logger.error('query text: %s', data.text);
      }
      if (err) {
        err.message = `in query ${data.name}: ${err.message}`;
        err.query = data;
      }
      next(err);
    });
  }

  function flush(cb) {
    if (!client) return cb();
    client.query('COMMIT', (err) => {
      if (manage) {
        client.end((e2) => cb(err || e2));
      } else {
        cb(err);
      }
    });
  }

  return miss.to.obj(write, flush);
}

module.exports = runQueries;
