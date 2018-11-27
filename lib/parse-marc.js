const assert = require('assert');
const stream = require('stream');
const events = require('events');
const expat = require('node-expat');
const miss = require('mississippi');
const log = require('gulplog');

const io = require('./io');

function createMarcParser() {
  var data, record, field, dfld, subfld;
  const parser = new expat.Parser('UTF-8');

  parser.on('startElement', start);
  parser.on('endElement', end);
  parser.on('text', text);

  return parser;

  function start(name, attrs) {
    if (name.startsWith('mx:')) name = name.slice(3);
    if (name === 'record') {
      record = {control: [], data: []};
    } else if (name === 'leader') {
      data = '';
    } else if (name === 'controlfield') {
      field = {tag: attrs.tag};
      data = '';
    } else if (name === 'datafield') {
      dfld = {tag: attrs.tag, ind1: attrs.ind1, ind2: attrs.ind2, subfields: []};
    } else if (name === 'subfield') {
      subfld = {code: attrs.code};
      data = '';
    }
  }

  function end(name) {
    if (name.startsWith('mx:')) name = name.slice(3);
    if (name === 'subfield') {
      subfld.data = data;
      dfld.subfields.push(subfld);
      data = undefined;
      subfld = undefined;
    } else if (name === 'datafield') {
      record.data.push(dfld);
      dfld = undefined;
    } else if (name === 'controlfield') {
      field.data = data;
      record.control.push(field);
      if (field.tag === '001') {
        record.cn = field.data.trim();
      }
      data = undefined;
      field = undefined;
    } else if (name === 'leader') {
      record.leader = data;
      data = undefined;
    } else if (name === 'record') {
      parser.emit('record', record);
      record = undefined;
    }
  }

  function text(txt) {
    if (data !== undefined) {
      data += txt;
    }
  }
}

class XMLToMarc extends stream.Transform {
  constructor() {
    super({objectMode: true});
    this.parser = createMarcParser();
    this.parser.on('error', (err) => this.destroy(err));

    this.parser.on('record', (rec) => {
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
  let data = buf.slice(ltab + 1);
  return {id: id, xml: data};
}

/**
 * Create a stream parser for VIAF-style MARC lines.
 */
function parseVIAFLines() {
  let lines = io.decodeLines(decodeIDLine);

  function parseRecord(line, enc, cb) {
    log.debug('parsing record %s: %d bytes', line.id, line.xml.length)
    let parser = createMarcParser();
    parser.on('error', (err) => {
        log.error('failed to parse viaf record %s: %s', line.id, err);
        return cb(err);
    });
    parser.on('record', (rec) => {
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
      log.debug('rendering record %s with id %s', rec.cn, id);
      nextId += 1;
      let fno = 0;
      if (rec.leader) {
        this.push([id, fno, 'LDR', null, null, null, escape(rec.leader)].join('\t') + '\n');
      }
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
