const zlib = require('zlib');
const stream = require('stream');
const fs = require('fs-extra');
const miss = require('mississippi');
const glob = require('glob');
const childProcess = require('child_process');
const StreamConcat = require('stream-concat');
const pg = require('pg');
const copyFrom = require('pg-copy-streams').from;

const log = require('gulplog');

const io = require('./io');
const throughput = require('./throughput');
const marc = require('./parse-marc');

function parseLOCFile(fn, stream, done) {
  let cp = childProcess.fork('./lib/loc-parse', [], {
    stdio: ['ignore', process.stdout, process.stderr, 'ipc']
  });

  cp.on('message', (msg) => {
    if (msg.$finished) {
      cp.send({exit: true});
      done();
    } else {
      if (!stream.write(msg)) {
        cp.send({pause: true});
      }
    }
  });
  stream.on('drain', () => {
    cp.send({resume: true});
  });
  cp.send({parse: fn});
}

function streamLOCFiles(pat) {
  let files = glob.sync(pat);
  log.info('processing %s LOC files', files.length);  
  let i = 0;

  function nextStream() {
    if (i >= files.length) return null;

    let fn = files[i];
    i += 1;
    log.info('opening %s', fn);

    return io.externalDecompress(fn)
             .pipe(marc.parseCollection());
  }

  return new StreamConcat(nextStream, {objectMode: true});
}

function importLOC(pat) {
  let cp = childProcess.spawn('psql', ['-c', '\\copy loc_marc_field FROM stdin'], {
    stdio: ['pipe', process.stdout, process.stderr]
  });

  let marcStream = marc.renderPostgresText();
  marcStream.pipe(cp.stdin);

  let files = glob.sync(pat);
  log.info('processing %s LOC files', files.length);  
  let i = 0;

  function advance() {
    if (i >= files.length) {
      marcStream.end();
    } else {
      let fn = files[i];
      i += 1;
      log.info('parsing %s', fn);
      parseLOCFile(fn, marcStream, advance);
    }
  }

  load.on('finish', () => client.end());
  process.nextTick(advance);
  
  return process.stdin;
}
module.exports.import = importLOC;

function convertLOCFiles(pat, outfile) {
  let combined = streamLOCFiles(pat);
  return combined.pipe(marc.renderPostgresText())
                 .pipe(zlib.createGzip())
                 .pipe(fs.createWriteStream(outfile));
}
module.exports.convert = convertLOCFiles;
