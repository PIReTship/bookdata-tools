"use strict";
const zlib = require('zlib');
const fs = require('fs');
const through = require('through2');
const fws = require('flush-write-stream');
const decodeLines = require('./lib/decode.js');

const options = require('yargs').argv;

const infn = options._[0];

const throughput = {
    start: process.hrtime(),
    nrecs: 0,
    tsize: 0,

    advance: function(n) {
        this.nrecs += 1;
        this.tsize += n;
        if (this.nrecs % 10000 == 0) {
            this.print();
        }
    },

    print: function() {
        var now = process.hrtime(this.start);
        var ftime = now[0] + now[1] * 1.0e-9;
        console.info("processed %d records in %ss (%srecs/s; average length %s)",
            this.nrecs, ftime.toFixed(3), (this.nrecs / ftime).toFixed(0),
            (this.tsize / this.nrecs).toFixed(0));
    }
};

fs.createReadStream(infn)
    .pipe(zlib.createUnzip())
    .pipe(decodeLines())
    .pipe(fws.obj((rec, enc, cb) => {
        throughput.advance(Object.keys(rec).length);
        cb();
    }), (cb) => {
        throughput.print();
        console.info("finished");
        cb();
    });