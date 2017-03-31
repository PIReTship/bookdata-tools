"use strict";
const fs = require('fs');
const ProgressBar = require('progress');
const path = require('path');
const through = require('through2');

function openFile(file, cb) {
    fs.stat(file, (err, stats) => {
        if (err) return cb(err);
        var size = stats.size;
        var bn = path.basename(file);
        var bar = new ProgressBar(bn + ' [:bar] :percent :etas', {
            total: size,
            width: 40
        });
        var stream = fs.createReadStream(file);
        cb(null, stream.pipe(through((chunk, enc, callback) => {
            bar.tick(chunk.length);
            callback(null, chunk, enc);
        })));
    });
}

function decodeLines() {
    var buffer = Buffer.alloc(16 * 1024);
    var used = 0;

    return through.obj(function(chunk, enc, callback) {
        // find new line
        var idx = chunk.indexOf('\n');
        while (idx >= 0) {
            let buf = chunk.slice(0, idx);
            if (used) {
                buf = Buffer.concat([buffer.slice(0, used), buf]);
                used = 0;
            }
            let ltab = buf.lastIndexOf('\t');
            if (ltab < 0) {
                return callback(new Error("no tab found in line"));
            }

            let data = buf.slice(ltab + 1).toString();
            let json = JSON.parse(data);
            if (idx < buf.length - 1) {
                buffer = buf.slice(idx + 1);
            }
            this.push(json);
            chunk = chunk.slice(idx + 1);
            idx = chunk.indexOf('\n');
        }

        if (chunk.length > 0) {
            if (used + chunk.length > buffer.length) {
                var nb = Buffer.alloc(Math.max(used + chunk.length, buffer.length * 2));
                buffer.copy(nb, 0, 0, used);
                buffer = nb;
            }
                
            chunk.copy(buffer, used);
            used += chunk.length;
        }

        callback();
    });
}

function decodeBadUnicode() {
  return through((chunk, enc, cb) => {
    const size = chunk.length;
    let nbuf = Buffer.alloc(size);
    let nbytes = 0;
    for (let i = 0; i < size; i++) {
      let u = chunk.readUInt8(i, true);
      if (u < 128) {
        nbuf.writeUInt8(u, nbytes, true);
        nbytes += 1;
      }
    }
    cb(null, chunk);
  });
}

module.exports = {
    openFile: openFile,
    decodeLines: decodeLines,
    decodeBadUnicode: decodeBadUnicode
};
