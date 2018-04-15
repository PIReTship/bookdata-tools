const zlib = require('zlib');
const fs = require('fs-extra');
const pg = require('pg');
const copyFrom = require('pg-copy-streams').from;

const io = require('./io');

const marc = require('./parse-marc');

async function importVIAF(file) {
  const client = new pg.Client();
  await client.connect();

  let stream = client.query(copyFrom('COPY viaf_marc_field FROM STDIN'));
  let input = io.openFile(infile);

  let promise = new Promise((ok, fail) => {
    stream.on('error', fail);
    stream.on('end', ok);
    input.on('error', fail);
  })

  input.pipe(zlib.createUnzip())
       .pipe(marc.parseVIAFLines())
       .pipe(marc.renderPostgresText())
       .pipe(stream);

  try {
    await promise;
  } finally {
    await client.end();
  }
}
module.exports.import = importVIAF;

function convertVIAF(infile, outfile) {
  return io.openFile(infile)
           .pipe(zlib.createUnzip())
           .pipe(marc.parseVIAFLines())
           .pipe(marc.renderPostgresText())
           .pipe(zlib.createGzip())
           .pipe(fs.createWriteStream(outfile));
}
module.exports.convert = convertVIAF;
