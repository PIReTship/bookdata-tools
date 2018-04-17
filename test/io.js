const assert = require('assert');
const stream = require('stream');

const fromValue = require('stream-from-value');

const io = require('../lib/io');

function b(s) {
  return new Buffer(s);
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
})
