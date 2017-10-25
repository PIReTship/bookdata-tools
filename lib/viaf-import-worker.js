const parseXml = require('@rgrove/parse-xml');
const pg = require('pg');
const runQueries = require('./query-eval-stream');

function text(elt) {
  let txt = '';
  for (let kid of elt.children) {
    if (kid.type == 'text') {
      txt += kid.text;
    } else if (kid.type == 'element') {
      txt += txt(kid);
    }
  }
  return txt;
}

function subfields(field) {
  let fields = {};
  for (let sf of field.children) {
    if (sf.type != 'element' || sf.name != 'mx:subfield') continue;

    fields[sf.attributes.code] = text(sf);
  }
  return fields;
}

function parseRecord(rec, enc, callback) {
  var xml = parseXml(rec.xml);
  var root = xml.children.find((e) => e.type == 'element');
  this.push({
    text: 'INSERT INTO viaf_author (viaf_au_key) VALUES ($1) RETURNING viaf_au_id',
    name: 'insert-author',
    values: [rec.id],
    returns: true
  });

  root.children.forEach((df) => {
    if (df.type != 'element' || df.name != 'mx:datafield') return;

    let data = subfields(df);
    if (df.attributes.tag == '700') {
      // Author Name
      this.push((r) => ({
        text: 'INSERT INTO viaf_author_name (viaf_au_id, viaf_au_name, viaf_au_name_type, viaf_au_name_dates, viaf_au_name_source) ' +
              'VALUES ($1, $2, $3, $4, $5)',
        name: 'insert-author-name',
        values: [r.viaf_au_id, data.a, df.ind1, data.d, data['2']]
      }));
    } else if (df.attributes.tag == '375') {
      // Author Gender
      this.push((r) => ({
        text: 'INSERT INTO viaf_author_gender (viaf_au_id, viaf_au_gender, viaf_au_gender_start, viaf_au_gender_end, viaf_au_gender_source) ' +
              'VALUES ($1, $2, $3, $4, $5)',
        name: 'insert-author-gender',
        values: [r.viaf_au_id, data.a, data.s, data.t, data['2']]
      }));
    }
  });

  process.nextTick(callback);
}
