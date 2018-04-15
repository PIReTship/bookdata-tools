const zlib = require('zlib');
const fs = require('fs-extra');

const io = require('./io');
const dbutil = require('./dbutil');
const marc = require('./parse-marc');

async function importVIAF(file, url) {
  let stream = io.openFile(file);
  let db = await dbutil.connect(url);

  try {
    await new Promise((ok, fail) => {
      stream.pipe(zlib.createUnzip())
        .pipe(marc.parseVIAFLines())
        .pipe(marc.importMarc(db, {
          records: 'viaf_marc_record',
          fields: 'viaf_marc_field',
          keys: {
            viaf_au_id: 'id'
          }
        }))
        .on('finish', ok)
        .on('error', fail);
    });
  } finally {
    await db.end();
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
