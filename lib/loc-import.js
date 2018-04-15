const zlib = require('zlib');
const fs = require('fs-extra');
const miss = require('mississippi');
const child_process = require('child_process');
const glob = require('glob');
const StreamConcat = require('stream-concat');

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
    log.info('finished parsing %s', file);
  } finally {
    await db.end();
  }
}
module.exports.import = importLOC;

function convertLOCFiles(pat, outfile) {
  let files = glob.sync(pat);
  log.info('processing %s LOC files', files.length);  
  let i = 0;

  function nextStream() {
    if (i >= files.length) return null;

    let fn = files[i];
    i += 1;
    log.info('opening %s', fn);

    return fs.createReadStream(fn)
             .pipe(zlib.createUnzip())
             .pipe(marc.parseCollection());
  }

  let combined = new StreamConcat(nextStream, {objectMode: true});
  return combined.pipe(marc.renderPostgresText())
                 .pipe(zlib.createGzip())
                 .pipe(fs.createWriteStream(outfile));
}
module.exports.convert = convertLOCFiles;
