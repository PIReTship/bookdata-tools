const assert = require('assert');
const stream = require('stream');
const events = require('events');
const expat = require('node-expat');
const miss = require('mississippi');
const log = require('gulplog');

const io = require('./io');

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
        this.record.cn = this.field.data.trim();
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

/**
 * Create a stream parser for a MARC-XML 'collection'.
 */
function parseCollection() {
  let parser = new expat.Parser('UTF-8');
  let mp = new MARCParser();
  let pt = miss.through.obj();
  let running = true;
  let drains = [];

  parser.on('startElement', mp.start.bind(mp));
  parser.on('endElement', mp.end.bind(mp));
  parser.on('text', mp.text.bind(mp));
  parser.on('error', (err) => pt.destroy(err));
  mp.on('record', (rec) => {
    if (!pt.write(rec)) {
      log.debug('pausing stream');
      running = false;
    }
  });
  pt.on('drain', () => {
    log.debug('resuming stream');
    running = true;
    while (drains.length) {
      let f = drains.shift();
      process.nextTick(f);
    }
  });

  let instr = new stream.Writable({
    write(chunk, enc, cb) {
      if (!parser.parse(chunk, false)) {
        return cb(parser.getError());
      }
      if (running) {
        cb();
      } else {
        log.debug('stream waiting');
        drains.push(cb);
      }
    },
    final(cb) {
      if (!parser.parse('', true)) {
        cb(parser.getError());
      } else {
        cb();
      }
    }
  });

  return miss.duplex.obj(instr, pt);
}
module.exports.parseCollection = parseCollection;

function decodeIDLine(buf) {
  let ltab = buf.indexOf('\t');
  if (ltab < 0) {
      throw new Error("no tab found in line");
  }

  let id = buf.slice(0, ltab).toString();
  let data = buf.slice(ltab + 1).toString();
  return {id: id, xml: data};
}

/**
 * Create a stream parser for VIAF-style MARC lines.
 */
function parseVIAFLines() {
  let lines = io.decodeLines(decodeIDLine);
  let mp = new MARCParser();
  let next;

  function parseRecord(line, enc, cb) {
    let parser = new expat.Parser('UTF-8');
    parser.on('startElement', mp.start.bind(mp));
    parser.on('endElement', mp.end.bind(mp));
    parser.on('text', mp.text.bind(mp));
    parser.on('error', (err) => cb(err));
    next = (rec) => {
      rec.id = line.id;
      parser.nextTick(cb, null, rec);
      next = null;
    }
    parser.write(line.xml);
  }
  
  mp.on('record', (rec) => {
    next(rec);
  });

  let parse = miss.through.obj(parseRecord);

  return miss.duplex.obj(lines, parse);
}
module.exports.parseVIAFLines = parseVIAFLines;

function importMarc(db, options) {
  let recKeys = [];
  Object.entries(options.keys).forEach(([k, v], i) => {
    recKeys.push({field: k, attr: v, num: i + 1});
  })
  let recQuery = {
    name: 'add-record',
    text: `INSERT INTO ${options.records} (${recKeys.map((k) => k.field).join(',')}) VALUES (${recKeys.map((k) => '$' + k.num).join(',')}) RETURNING rec_id`
  };
  let fldQuery = {
    name: 'add-field',
    text: `INSERT INTO ${options.fields} (rec_id, fld_no, tag, ind1, ind2, sf_code, contents) VALUES ($1, $2, $3, $4, $5, $6, $7)`
  };
  let n = 0;

  async function addRecord(rec) {
    await db.query({
      name: 'begin',
      text: 'BEGIN ISOLATION LEVEL READ UNCOMMITTED'
    });
    n += 1;
    let rvs = recKeys.map((k) => rec[k.attr]);
    let recResult = await db.query(Object.assign({values: rvs}, recQuery));
    let recId = recResult.rows[0].rec_id;
    let fno = 0;
    let fps = [];
    for (let cf of rec.control) {
      fno += 1;
      let values = [recId, fno, cf.tag, null, null, null, cf.data];
      fps.push(db.query(Object.assign({values: values}, fldQuery)));
    }
    for (let df of rec.data) {
      fno += 1;
      for (let sf of df.subfields) {
        let values = [recId, fno, df.tag, df.ind1, df.ind2, sf.code, sf.data];
        fps.push(db.query(Object.assign({values: values}, fldQuery)));
      }
    }
    await Promise.all(fps);
    await db.query('COMMIT');
    log.debug('committed %s', rec.cn);
  }

  return miss.to.obj((rec, enc, cb) => {
    addRecord(rec).then(() => cb(), (err) => cb(err));
  }, (cb) => {
    db.query('COMMIT', cb);
  });
}
module.exports.importMarc = importMarc;
