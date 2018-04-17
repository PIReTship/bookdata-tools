"use strict";

const util = require('util');
const zlib = require('zlib');
const fs = require('fs');
const through = require('through2');
const pg = require('pg');
const async = require('async');
const logger = require('gulplog');

const throughput = require('./throughput');
const io = require('./io');
const runQueries = require('./query-eval-stream');

var ninserts = 0;

var autp = throughput('authors');
var wtp = throughput('works');
var etp = throughput('editions');

function decodeLine(buf) {
    let ltab = buf.lastIndexOf('\t');
    if (ltab < 0) {
        throw new Error("no tab found in line");
    }

    let data = buf.slice(ltab + 1).toString();
    let json = JSON.parse(data);
    return json;
}

const imports = {
  authors: function (rec) {
    return {
      text: 'INSERT INTO ol_author (author_key, author_name, author_data) VALUES ($1, $2, $3)',
      name: 'insert-author',
      values: [rec.key, rec.name, JSON.stringify(rec)]
    };
  },
  works: function (rec) {
    return {
      text: 'INSERT INTO ol_work (work_key, work_title, work_data) VALUES ($1, $2, $3)',
      name: 'insert-work',
      values: [rec.key, rec.title, JSON.stringify(rec)]
    };
  },
  editions: function(rec) {
    return {
      text: 'INSERT INTO ol_edition (edition_key, edition_title, edition_data) VALUES ($1, $2, $3)',
      name: 'insert-edition',
      values: [rec.key, rec.title, JSON.stringify(rec)]
    };
  }
};

async function doImport(name, date) {
  const proc = imports[name];
  if (proc === undefined) {
    return callback(new Error("no such import " + name));
  }
  const client = new pg.Client();
  await client.connect();
  let resP = new Promise((ok, fail) => {
    io.openFile(util.format("data/ol_dump_%s_%s.txt.gz", name, date), cb)
      .pipe(zlib.createUnzip())
      .pipe(io.decodeLines(decodeLine))
      .pipe(through.obj((rec, enc, cb) => {
        cb(null, proc(rec));
      }))
      .pipe(runQueries(client, {logger: logger}))
      .on('finish', () => ok())
      .on('error', fail);
  });

  try {
    await resP;
  } finally {
    await client.end();
  }
}

for (let name of Object.keys(imports)) {
  module.exports[name] = doImport.bind(null, name);
}
