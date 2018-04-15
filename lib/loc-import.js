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

function streamLOCFiles() {
  let files = glob.sync(pat);
  log.info('processing %s LOC files', files.length);  
  let i = 0;

  function nextStream() {
    if (i >= files.length) return null;

    let fn = files[i];
    i += 1;
    log.info('opening %s', fn);

    return io.openFile(fn)
             .pipe(zlib.createUnzip())
             .pipe(marc.parseCollection());
  }

  return new StreamConcat(nextStream, {objectMode: true});
}

async function importLOC(file, url) {
  const client = new pg.Client();
  await client.connect();

  let stream = client.query(copyFrom('COPY loc_marc_field FROM STDIN'));
  let input = streamLOCFiles();

  let promise = new Promise((ok, fail) => {
    stream.on('error', fail);
    stream.on('end', ok);
    input.on('error', fail);
  })

  input.pipe(marc.renderPostgresText())
       .pipe(stream);

  try {
    await promise;
  } finally {
    await client.end();
  }
}
module.exports.import = importLOC;

function convertLOCFiles(pat, outfile) {
  let combined = streamLOCFiles(pat);
  return combined.pipe(marc.renderPostgresText())
                 .pipe(zlib.createGzip())
                 .pipe(fs.createWriteStream(outfile));
}
module.exports.convert = convertLOCFiles;
