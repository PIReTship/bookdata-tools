const fs = require('fs');
const zlib = require('zlib');
const path = require('path');
const stream = require('stream');
const progress = require('progress-stream');
const Gauge = require('gauge');

const marc = require('./parse-marc');

let paused = false;
let resumes = [];

function parseFile(fn) {
  console.error('parsing file %s', fn);
  let stat = fs.statSync(fn);
  let size = stat.size;
  let name = path.basename(fn);
  let prog = progress({length: size});
  let gauge = new Gauge({
    template: [
      {type: 'progressbar', length: 20},
      {type: 'activityIndicator', kerning: 1, length: 1},
      {type: 'section', kerning: 1, default: ''},
      {type: 'subsection', kerning: 1, default: ''},
      {type: 'eta', kerning: 1, default: ''}
    ]
  });
  prog.on('progress', (p) => {
    gauge.show({
      section: name,
      completed: p.percentage * 0.01,
      eta: p.eta
    });
  });
  gauge.show(name);
  fs.createReadStream(fn)
    .pipe(prog)
    .pipe(zlib.createUnzip())
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
        gauge.hide();
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
