"use strict";

const parseXml = require('@rgrove/parse-xml');
const zlib = require('zlib');
const fs = require('fs');
const through = require('through2');
const pg = require('pg');
const async = require('async');
const gutil = require('gulp-util');
const runQueries = require('./query-eval-stream');
const io = require('./io');

function decodeLine(buf) {
  let ltab = buf.indexOf('\t');
  if (ltab < 0) {
      throw new Error("no tab found in line");
  }

  let id = buf.slice(0, ltab).toString();
  let data = buf.slice(ltab + 1).toString();
  return {id: id, xml: data};
}

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
    text: 'INSERT INTO viaf_author (viaf_au_id) VALUES ($1)',
    name: 'insert-author',
    values: [rec.id]
  });

  for (let df of root.children) {
    if (df.type != 'element' || df.name != 'mx:datafield') continue;

    let data = subfields(df);
    if (df.attributes.tag == '700') {
      // Author Name
      this.push({
        text: 'INSERT INTO (viaf_au_id, viaf_au_name, viaf_au_name_type, viaf_au_name_dates, viaf_au_name_source) VALUES ($1, $2, $3, $4, $5)',
        name: 'insert-author-name',
        values: [rec.id, data.a, df.ind1, data.d, data['2']]
      });
    } else if (df.attributes.tag == '375') {
      // Author Gender
      this.push({
        text: 'INSERT INTO (viaf_au_id, viaf_au_gender, viaf_au_gender_start, viaf_au_gender_end, viaf_au_gender_source) VALUES ($1, $2, $3, $4, $5)',
        name: 'insert-author-name',
        values: [rec.id, data.a, data.s, data.t, data['2']]
      });
    }
  }

  process.nextTick(callback);
}

function doImport(file, callback) {
  const client = new pg.Client(gutil.env['db-url']);

  async.waterfall([
    (next) => client.connect(next),
    (cb) => io.openFile(file, cb),
    (stream, next) => {
      stream.pipe(zlib.createUnzip())
            .pipe(io.decodeLines(decodeLine))
            .pipe(through.obj(parseRecord))
            .pipe(runQueries(client, next));
    }
  ], (err) => {
    if (err) {
      console.error("error running %s: %s", name, err);
    } else {
      console.info("finished %s", name);
    }
    client.end((e2) => {
      if (err) {
        callback(err);
      } else if (e2) {
        callback(e2);
      } else {
        callback();
      }
    });
  });
}

module.exports.import = doImport;
