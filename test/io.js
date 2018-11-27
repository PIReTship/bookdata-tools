const assert = require('assert');
const stream = require('stream');
const crypto = require('crypto');
const expect = require('expect.js');
const miss = require('mississippi');

const fromValue = require('stream-from-value');

const io = require('../lib/io');

function randomStream(bytes) {
  let total = 0;
  
  return miss.from(function(size, next) {
    let max = Math.min(size, bytes - total);
    let n = Math.random() * 412 + 100;
    n = Math.min(n, max);
    if (n == 0) {
        return next(null, null);
    }
    crypto.randomBytes(n, (err, buf) => {
      total += buf.length;
      next(err, buf);
    });
  });
}

function b(s) {
  return Buffer.from(s);
}

function makeArray(done) {
  let array = [];

  return new stream.Writable({
    objectMode: true,
    write(chunk, enc, cb) {
      array.push(chunk);
      cb();
    },

    final(cb) {
      process.nextTick(done, null, array);
      cb();
    }
  });
}

describe('decodeLines()', () => {
  it('should be empty on an empty string', (done) => {
    fromValue('').pipe(io.decodeLines())
                 .pipe(makeArray((err, arr) => {
      if (err) return done(err);
      assert.equal(arr.length, 0);
      done();
    }));
  });

  it('should split a line', (done) => {
    fromValue('foo\n').pipe(io.decodeLines())
                 .pipe(makeArray((err, arr) => {
      if (err) return done(err);
      assert.equal(arr.length, 1);
      assert.equal(arr[0], 'foo');
      done();
    }));
  });

  it('should pass a partial line', (done) => {
    fromValue('bar').pipe(io.decodeLines())
                 .pipe(makeArray((err, arr) => {
      if (err) return done(err);
      assert.equal(arr.length, 1);
      assert.equal(arr[0], 'bar');
      done();
    }));
  });

  it('should pass a couple lines', (done) => {
    fromValue('wumpus\nsplintercat\n').pipe(io.decodeLines())
                 .pipe(makeArray((err, arr) => {
      if (err) return done(err);
      assert.deepEqual(arr, ['wumpus', 'splintercat']);
      done();
    }));
  });

  it('should pass full & partial', (done) => {
    fromValue('wumpus\nsplintercat').pipe(io.decodeLines())
                 .pipe(makeArray((err, arr) => {
      if (err) return done(err);
      assert.deepEqual(arr, ['wumpus', 'splintercat']);
      done();
    }));
  });

  it('should pass chunks', (done) => {
    let lines = io.decodeLines();
    lines.pipe(makeArray((err, arr) => {
      if (err) return done(err);
      assert.deepEqual(arr, ['wumpus', 'splintercat']);
      done();
    }));
    lines.write(b('wum'));
    lines.write(b('pus\nsplint'));
    lines.write(b('ercat\n'));
    lines.end();
  });

  it('should handle a partial chunk', (done) => {
    let lines = io.decodeLines();
    lines.pipe(makeArray((err, arr) => {
      if (err) return done(err);
      assert.deepEqual(arr, ['wumpus', 'splintercat']);
      done();
    }));
    lines.write(b('wum'));
    lines.write(b('pus\nsplint'));
    lines.write(b('ercat'));
    lines.end();
  });

  it('should handle a chunks and a write', (done) => {
    let lines = io.decodeLines();
    lines.pipe(makeArray((err, arr) => {
      if (err) return done(err);
      assert.deepEqual(arr, ['wumpus', 'splintercat']);
      done();
    }));
    lines.write(b('wum'));
    lines.write(b('pus\nsplint'));
    lines.write(b('ercat\n'));
    lines.write(b(''));
    lines.end();
  });
  
  it('should handle some json', (done) => {
    fromValue(b('"wumpus"\n"splintercat"\n')).pipe(io.decodeLines(JSON.parse))
                 .pipe(makeArray((err, arr) => {
      if (err) return done(err);
      assert.deepEqual(arr, ['wumpus', 'splintercat']);
      done();
    }));
  });
  
  it('should handle many random bytes', (done) => {
    let nbytes = 100000;
    randomStream(nbytes).pipe(io.decodeLines((b) => b.length))
                        .pipe(makeArray((err, arr) => {
                           if (err) return done(err);
                           let len = 0;
                           for (let e of arr) {
                             len += e + 1
                           }
                           // allow for missing/present final newline
                           // if we have a final newline: then each thing has a newline
                           // if we do not: then we are short one newline
                           expect(len).to.be.below(nbytes+2);
                           expect(len).to.be.above(nbytes-1);
                           done();
                        }));

  });
})
