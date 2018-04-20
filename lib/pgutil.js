const stream = require('stream');

function escape(txt) {
  let buf = Buffer.isBuffer(txt) ? txt : Buffer.from(txt, 'utf-8');
  const n = buf.length;
  let tgt = Buffer.allocUnsafe(n * 2);
  let opos = 0;
  for (let i = 0; i < n; i++) {
    let b = buf[i];
    switch (b) {
      case 92: // backslash
        tgt[opos++] = 92;
        tgt[opos++] = 92;
        break;
      case 9: // tab
        tgt[opos++] = 92;
        tgt[opos++] = 116;
        break;
      case 10: // newline
        tgt[opos++] = 92;
        tgt[opos++] = 110;
        break;
      case 13: // carriage return
        tgt[opos++] = 92;
        tgt[opos++] = 114;
        break;
      default:
        tgt[opos++] = b;
    }
  }

  return tgt.slice(0, opos);
}
module.exports.escapePGText = escape;

class PGEncode extends stream.Transform {
  constructor() {
    super();
  }

  _write(rec, enc, cb) {
    let n = rec.length;
    for (let i = 0; i < n; i++) {
      let f = rec[i];
      if (i > 0) this.push('\t');
      if (f == null) {
        this.push('\\N');
      } else if (typeof f == 'string' || f instanceof Buffer) {
        this.push(escape(f));
      } else {
        this.push(escape(f.toString()));
      }
    }
    this.push('\n');
    cb();
  }
}

module.exports.encodePGText = function() {
  return new PGEncode();
}
