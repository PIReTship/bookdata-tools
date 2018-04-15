const assert = require('assert');
const stream = require('stream');
const events = require('events');
const expat = require('node-expat');
const miss = require('mississippi');
const log = require('gulplog');

const io = require('./io');

class MARCParser extends events.EventEmitter {
  start(name, attrs) {
    if (name.startsWith('mx:')) name = name.slice(3);
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
    if (name.startsWith('mx:')) name = name.slice(3);
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

class XMLToMarc extends stream.Transform {
  constructor() {
    super({objectMode: true});
    this.marc = new MARCParser();
    this.parser = new expat.Parser('UTF-8');
    this.parser.on('startElement', this.marc.start.bind(this.marc));
    this.parser.on('endElement', this.marc.end.bind(this.marc));
    this.parser.on('text', this.marc.text.bind(this.marc));
    this.parser.on('error', (err) => this.destroy(err));

    this.marc.on('record', (rec) => {
      this.push(rec);
    });
  }

  _transform(chunk, enc, cb) {
    if (!this.parser.parse(chunk, false)) {
      return cb(this.parser.getError());
    } else {
      return cb();
    }
  }

  _final(cb) {
    if (!this.parser.parse('', true)) {
      return cb(this.parser.getError());
    } else {
      return cb();
    }
  }
}

/**
 * Create a stream parser for a MARC-XML 'collection'.
 */
function parseCollection() {
  return new XMLToMarc();
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

  function parseRecord(line, enc, cb) {
    let parser = new expat.Parser('UTF-8');
    let mp = new MARCParser();

    parser.on('startElement', mp.start.bind(mp));
    parser.on('endElement', mp.end.bind(mp));
    parser.on('text', mp.text.bind(mp));
    parser.on('error', (err) => cb(err));
    mp.on('record', (rec) => {
      log.debug('finished record %s', line.id);
      rec.id = line.id;
      process.nextTick(cb, null, rec);
    });
    parser.write(line.xml);
  }

  let parse = miss.through.obj(parseRecord);
  lines.pipe(parse);

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
    if (n % 1000 == 0) {
      if (n > 0) {
        await db.query('COMMIT');
      }
    await db.query({
      name: 'begin',
      text: 'BEGIN ISOLATION LEVEL READ UNCOMMITTED'
    });
    }
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
    log.debug('committed %s', rec.cn);
  }

  return miss.to.obj((rec, enc, cb) => {
    addRecord(rec).then(() => cb(), (err) => cb(err));
  }, (cb) => {
    db.query('COMMIT', cb);
  });
}
module.exports.importMarc = importMarc;

/**
 * Render MARC data to a PostgreSQL text file suitable for COPY FROM.
 */
function renderPostgresText(options) {
  let nextId = (options && options.initialId) || 1;

  function escape(txt) {
    if (txt == null) return '\\N';

    return txt.replace(/\\/g, '\\\\')
              .replace(/\r/g, '')
              .replace(/\n/g, '\\n')
              .replace(/\t/g, '\\t');
  }

  return new stream.Transform({
    objectMode: true,
    
    transform(rec, encoding, cb) {
      let id = nextId;
      nextId += 1;
      let fno = 0;
      for (let cf of rec.control) {
        fno += 1;
        this.push([id, fno, cf.tag, null, null, null, escape(cf.data)].join('\t') + '\n');
      }
      for (let df of rec.data) {
        fno += 1;
        for (let sf of df.subfields) {
          this.push([id, fno, df.tag, df.ind1, df.ind2, sf.code, escape(sf.data)].join('\t') + '\n');
        }
      }
      cb();
    }
  });
}
module.exports.renderPostgresText = renderPostgresText;
