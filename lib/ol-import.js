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
const runQueries = require('./lib/query-eval-stream');
const gutil = require('gulp-util');

const date = gutil.env.date || '2016-07-31';

var ninserts = 0;

var autp = throughput('authors');
var wtp = throughput('works');
var etp = throughput('editions');

function decodeLine(buf) {
    let ltab = buf.lastIndexOf('\t');
    if (ltab < 0) {
        return callback(new Error("no tab found in line"));
    }

    let data = buf.slice(ltab + 1).toString();
    let json = JSON.parse(data);
    return json;
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
  const client = new pg.Client(gutil.env['db-url']);

  async.waterfall([
    client.connect.bind(client),
    (_, cb) => io.openFile(util.format("data/ol_dump_%s_%s.txt.gz", name, date), cb),
    (stream, cb) => {
      stream.pipe(zlib.createUnzip())
            .pipe(io.decodeLines(decodeLine))
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

for (let name of Object.keys(imports)) {
  module.exports[name] = doImport.bind(null, name);
}
