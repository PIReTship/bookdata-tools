const fs = require('fs');
const zlib = require('zlib');
const path = require('path');
const stream = require('stream');

const io = require('./io');
const marc = require('./parse-marc');

let paused = false;
let resumes = [];

function parseFile(fn) {
  console.error('parsing file %s', fn);
  io.openFile(fn)
    .pipe(zlib.createUnzip())
    .pipe(marc.parseCollection())
    .pipe(new stream.Transform({
      objectMode: true,
      transform(rec, enc, cb) {
        cb(null, JSON.stringify(rec) + '\n');
      }
    }))
    .pipe(process.stdout);
}

parseFile(process.argv[2]);
