const zlib = require('zlib');
const fs = require('fs-extra');
const miss = require('mississippi');

const io = require('./io');
const throughput = require('./throughput');
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
