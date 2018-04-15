const stream = require('stream');
const events = require('events');
const expat = require('node-expat');
const miss = require('mississippi');
const log = require('gulplog');

class MARCParser extends events.EventEmitter {
  start(name, attrs) {
    if (name == 'record') {
      this.record = {control: [], data: []};
    } else if (name == 'controlfield') {
      this.field = {tag: attrs.tag};
      this.data = '';
    } else if (name == 'datafield') {
      this.dfld = {tag: attrs.tag, ind1: attrs.ind1, ind2: attrs.ind2, subfields: []};
    } else if (name == 'subfield') {
      this.subfld = {code: attrs.code};
      this.data = '';
    }
  }

  end(name) {
    if (name == 'subfield') {
      this.subfld.data = this.data;
      this.dfld.subfields.push(this.subfld);
      delete this.data;
      delete this.subfld;
    } else if (name == 'datafield') {
      this.record.data.push(this.dfld);
      delete this.dfld;
    } else if (name == 'controlfield') {
      this.field.data = this.data;
      this.record.control.push(this.field);
      if (this.field.tag == '001') {
        this.record.lccn = this.field.data.trim();
      }
      delete this.data;
      delete this.field;
    } else if (name == 'record') {
      this.emit('record', this.record);
      delete this.record;
    }
  }

  text(txt) {
    if (this.data !== undefined) {
      this.data += txt;
    }
  }
}

function parseEntries() {
  let parser = new expat.Parser('UTF-8');
  let mp = new MARCParser();
  let pt = miss.through.obj();

  function error(err) {
    throw err;
  }

  parser.on('startElement', mp.start.bind(mp));
  parser.on('endElement', mp.end.bind(mp));
  parser.on('text', mp.text.bind(mp));
  parser.on('error', error);
  mp.on('record', (rec) => {
    if (!pt.write(rec)) {
      parser.stop();
    }
  });
  pt.on('drain', () => parser.resume());

  return miss.duplex.obj(parser, pt);
}

module.exports.parseEntries = parseEntries;
