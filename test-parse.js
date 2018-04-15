let fs = require('fs-extra');
let zlib = require('zlib');
let marc = require('./lib/parse-marc');
let miss = require('mississippi');

let start = process.hrtime();
let n = 0;

fs.createReadStream('data/LOC/BooksAll.2014.part01.xml.gz')
  .pipe(zlib.createGunzip())
  .pipe(marc.parseEntries())
  .pipe(miss.to.obj((rec, enc, cb) => {
    n += 1;
    if (n % 500 == 0) {
      let [es, ems] = process.hrtime(start);
      let t = es + ems * 1e-9;
      let rate = n / t;
      console.log('parsed %d at %s r/s', n, rate.toPrecision(5));
    }
    cb();
  }));
