const zlib = require('zlib');
const fs = require('fs-extra');
const child_process = require('child_process');

const io = require('./io');
const dbutil = require('./dbutil');
const marc = require('./parse-marc');

function importVIAF(file) {
  let cp = child_process.spawn('psql', ['-c', '\\copy viaf_marc_field FROM STDIN'], {
    stdio: ['pipe', 'inherit', 'inherit']
  });

  return io.openFile(infile)
           .pipe(zlib.createUnzip())
           .pipe(marc.parseVIAFLines())
           .pipe(marc.renderPostgresText())
           .pipe(cp.stdin);
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
