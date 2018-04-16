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
  io.externalDecompress(fn)
    .pipe(marc.parseCollection())
    .pipe(new stream.Writable({
      objectMode: true,
      write(rec, enc, cb) {
        process.send(rec);
        if (paused) {
          resumes.push(cb);
        } else {
          process.nextTick(cb);
        }
      },

      final(cb) {
        process.send({$finished: true});
        cb();
      }
    }))
}

process.on('message', (msg) => {
  if (msg.pause) {
    paused = true;
  } else if (msg.resume) {
    paused = false;
    while (resumes.length) {
      process.nextTick(resumes.shift());
    }
  } else if (msg.parse) {
    process.nextTick(parseFile, msg.parse);
  } else if (msg.exit) {
    process.exit(0);
  }
});
