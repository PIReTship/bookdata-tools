const zlib = require('zlib');
const fs = require('fs-extra');

const io = require('./io');
const dbutil = require('./dbutil');
const marc = require('./parse-marc');

async function importLOC(file, url) {
  let stream = io.openFile(file);
  let db = await dbutil.connect(url);

  try {
    await new Promise((ok, fail) => {
      stream.pipe(zlib.createUnzip())
        .pipe(marc.parseCollection())
        .pipe(marc.importMarc(db, {
          records: 'loc_marc_record',
          fields: 'loc_marc_field',
          keys: {
            lccn: 'cn'
          }
        }))
        .on('finish', ok)
        .on('error', fail);
    });
  } finally {
    await db.end();
  }
}

module.exports.import = importLOC;
