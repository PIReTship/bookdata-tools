"use strict";
const fs = require('fs');
const ProgressBar = require('progress');
const path = require('path');
const through = require('through2');

function openFile(file, cb) {
  var bar;
  var consumed = 0;
  let res = through((chunk, enc, callback) => {
    if (bar) {
      bar.tick(chunk.length + consumed);
      consumed = 0;
    } else {
      consumed += chunk.length;
    }
    callback(null, chunk, enc);
  });
  fs.stat(file, (err, stats) => {
    if (err) {
      if (cb) {
        return cb(err);
      } else {
        throw err;
      }
    }
    var size = stats.size;
    var bn = path.basename(file);
    bar = new ProgressBar(bn + ' [:bar] :percent :etas', {
      total: size,
      width: 40
    });
    var stream = fs.createReadStream(file);
    stream.pipe(res);
    if (cb) {
      cb(null, res);
    }
  });
  return res;
}

function decodeLines(decode) {
    var buffer = Buffer.alloc(16 * 1024);
    var used = 0;
    var dcf = decode || ((b) => b.toString());

    return through.obj(function(chunk, enc, callback) {
        // find new line
        var idx = chunk.indexOf('\n');
        while (idx >= 0) {
            let buf = chunk.slice(0, idx);
            if (used) {
                buf = Buffer.concat([buffer.slice(0, used), buf]);
                used = 0;
            }
            
            try {
                let payload = dcf(buf);
                this.push(payload);
            } catch (e) {
                return callback(e);
            }

            if (idx < buf.length - 1) {
                buffer = buf.slice(idx + 1);
            }
        
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
