const assert = require('assert');

const pgu = require('../lib/pgutil');

describe('escape', () => {
  it('should pass through an empty string', () => {
    assert.equal(pgu.escapePGText('').length, 0);
  })
  it('should pass through an empty buffer', () => {
    assert.equal(pgu.escapePGText('').length, 0);
  })

  it('should pass through text', () => {
    assert.equal(pgu.escapePGText('woozle').toString(), 'woozle');
  })
  it('should escape a backslash', () => {
    assert.equal(pgu.escapePGText('\\').toString(), '\\\\');
  })
  it('should escape a newline', () => {
    assert.equal(pgu.escapePGText('\n').toString(), '\\n');
  })
  it('should escape a carriage return', () => {
    assert.equal(pgu.escapePGText('\r').toString(), '\\r');
  })
  it('should escape a tab', () => {
    assert.equal(pgu.escapePGText('\t').toString(), '\\t');
  })
  it('should escape a tab in text', () => {
    assert.equal(pgu.escapePGText('foo\tbar').toString(), 'foo\\tbar');
  })
  it('should escape stuff in text', () => {
    assert.equal(pgu.escapePGText('foo\tbar\r\n\\foobat').toString(), 'foo\\tbar\\r\\n\\\\foobat');
  })
})
