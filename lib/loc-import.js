const zlib = require('zlib');
const stream = require('stream');
const fs = require('fs-extra');
const miss = require('mississippi');
const glob = require('glob');
const childProcess = require('child_process');
const StreamConcat = require('stream-concat');
const pg = require('pg');

const log = require('gulplog');

const io = require('./io');
const throughput = require('./throughput');
const marc = require('./parse-marc');
const pgutil = require('./pgutil');

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

  let marcStream = marc.renderPostgresText();
  let output = pgutil.openCopyStream('loc_marc_field');
  marcStream.pipe(output);
  streamLOCFiles(pat).pipe(marcStream);

  return output;
}
module.exports.import = importLOC;

function convertLOCFiles(pat, outfile) {
  let combined = streamLOCFiles(pat);
  return combined.pipe(marc.renderPostgresText())
                 .pipe(zlib.createGzip())
                 .pipe(fs.createWriteStream(outfile));
}
module.exports.convert = convertLOCFiles;
