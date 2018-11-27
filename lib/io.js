const fs = require('fs');
const stream = require('stream');
const childProcess = require('child_process');
const ProgressBar = require('progress');
const path = require('path');
const through = require('through2');

function openFile(file, cb) {
  var bar;
  var consumed = 0;
  let res = new stream.Transform({
    transform(chunk, enc, callback) {
      if (bar) {
        bar.tick(chunk.length + consumed);
        consumed = 0;
      } else {
        consumed += chunk.length;
      }
      callback(null, chunk, enc);
    }
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
    bar = new ProgressBar(bn + ' [:bar] :percent :elapseds (ETA :etas)', {
      total: size,
      width: 30,
      renderThrottle: 100
    });
    var stream = fs.createReadStream(file);
    stream.pipe(res);
    if (cb) {
      cb(null, res);
    }
  });
  return res;
}

function externalDecompress(file) {
  let name = path.basename(file);
  let gz = childProcess.spawn('gzip', ['-d'], {
    stdio: ['pipe', 'pipe', process.stderr]
  });
  let pv = childProcess.spawn('pv', ['-N', name, file], {
    stdio: ['ignore', gz.stdin, process.stderr]
  });

  return gz.stdout;
}

function decodeLines(decode) {
  var buffer = Buffer.alloc(16 * 1024);
  var used = 0;
  var dcf = decode || ((b) => b.toString());

  return new stream.Transform({
    objectMode: true,
    transform(chunk, enc, callback) {
      if (typeof(chunk) === 'string') {
        chunk = Buffer.from(chunk);
      }
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
    },
    
    flush(cb) {
      if (used) {
        this.push(dcf(buffer.slice(0, used)));
      }
      cb();
    }
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
    decodeBadUnicode: decodeBadUnicode,
    externalDecompress: externalDecompress
};
