const zlib = require('zlib');
const fs = require('fs-extra');
const miss = require('mississippi');
const child_process = require('child_process');

const log = require('gulplog');

const io = require('./io');
const throughput = require('./throughput');
const dbutil = require('./dbutil');
const marc = require('./parse-marc');

async function importLOC(file, url) {
  let cp = child_process.fork('./lib/loc-parse', [], {
    stdio: ['ignore', process.stdout, process.stderr, 'ipc']
  });
  let db = await dbutil.connect(url);
  let im = marc.importMarc(db, {
    records: 'loc_marc_record',
    fields: 'loc_marc_field',
    keys: {
      lccn: 'cn'
    }
  });

  try {
    await new Promise((ok, fail) => {
      cp.on('error', fail);
      im.on('drain', () => {
        cp.send({resume: true});
      });
      cp.on('message', (msg) => {
        if (msg.$finish) {
          im.end();
          cp.send({exit: true});
          ok();
        } else {
          if (!im.write(msg)) {
            cp.send({pause: true});
          }
        };
      });
      log.info('requesting parse of %s', file);
      cp.send({parse: file});
    });
  } finally {
    await db.end();
  }
}
module.exports.import = importLOC;

function convertLOCFiles() {
  let marcOut = marc.renderPostgresText();
  
  let input = miss.through.obj((file, enc, cb) => {
    let stream = file.contents;
    stream.on('end', cb);
    stream.on('error', cb);
    stream.pipe(zlib.createUnzip())
          .pipe(marc.parseCollection())
          .pipe(marcOut, {end: false});
  }, (cb) => {
    marcOut.end(null, null, cb);
  });
  return miss.duplex.obj(input, marcOut);
}
module.exports.convert = convertLOCFiles;
