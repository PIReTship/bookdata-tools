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

function parseLOCFile(fn) {
  let cp = childProcess.fork('./lib/loc-parse', [fn], {
    stdio: ['ignore', 'pipe', process.stderr, 'ipc']
  });

  return cp.stdout.pipe(io.decodeLines(JSON.parse));
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

    return parseLOCFile(fn);
  }

  return new StreamConcat(nextStream, {objectMode: true});
}

function importLOC(pat) {
  let cp = childProcess.spawn('psql', ['-c', '\\copy loc_marc_field FROM stdin'], {
    stdio: ['pipe', process.stdout, process.stderr]
  });

  let marcStream = marc.renderPostgresText();
  marcStream.pipe(cp.stdin);
  streamLOCFiles(pat).pipe(marcStream);

  return cp.stdin;
}
module.exports.import = importLOC;

function convertLOCFiles(pat, outfile) {
  let combined = streamLOCFiles(pat);
  return combined.pipe(marc.renderPostgresText())
                 .pipe(zlib.createGzip())
                 .pipe(fs.createWriteStream(outfile));
}
module.exports.convert = convertLOCFiles;
