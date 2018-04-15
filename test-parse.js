let fs = require('fs-extra');
let zlib = require('zlib');
let marc = require('./lib/parse-marc');
let miss = require('mississippi');
let Gauge = require('gauge');

let gauge = new Gauge({updateInterval: 250});
let start = process.hrtime();
let n = 0;

fs.createReadStream('data/LOC/BooksAll.2014.part01.xml.gz')
  .pipe(zlib.createGunzip())
  .pipe(marc.parseEntries())
  .pipe(miss.to.obj((rec, enc, cb) => {
    n += 1;
    gauge.pulse();
    if (n % 100 == 0) {
      let [es, ems] = process.hrtime(start);
      let t = es + ems * 1e-9;
      let rate = n / t;
      gauge.show('parsing: ' + rate.toPrecision(5) + ' r/s');
    }
    cb();
  }));
