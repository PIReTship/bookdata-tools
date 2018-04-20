const util = require('util');
const zlib = require('zlib');
const fs = require('fs');
const through = require('through2');
const stream = require('stream');
const childProcess = require('child_process');
const pg = require('pg');
const async = require('async');
const logger = require('gulplog');

const throughput = require('./throughput');
const io = require('./io');
const runQueries = require('./query-eval-stream');
const pgu = require('./pgutil');

var ninserts = 0;

var autp = throughput('authors');
var wtp = throughput('works');
var etp = throughput('editions');

function decodeLine(buf) {
    let ltab = buf.lastIndexOf('\t');
    if (ltab < 0) {
        throw new Error("no tab found in line");
    }

    let data = buf.slice(ltab + 1).toString();
    let json = JSON.parse(data);
    return json;
}

const imports = {
  authors: {
    table: 'ol_author',
    pfx: 'author',
    label_field: 'author_name',
    label: 'name'
  },
  works: {
    table: 'ol_work',
    pfx: 'work',
    label_field: 'work_title',
    label: 'title'
  },
  editions: {
    table: 'ol_edition',
    pfx: 'edition',
    label_field: 'edition_title',
    label: 'title'
  }
};

async function doImport(name, date) {
  const def = imports[name];
  if (def === undefined) {
    throw new Error("no such import " + name);
  }

  let cp = childProcess.spawn('psql', ['-c', `\\copy ${def.table} (${def.pfx}_key, ${def.label_field}, ${def.pfx}_data) FROM STDIN`], {
    stdio: ['pipe', process.stdout, process.stderr]
  });
  
  let resP = new Promise((ok, fail) => {
    io.openFile(util.format("data/ol_dump_%s_%s.txt.gz", name, date))
      .pipe(zlib.createUnzip())
      .pipe(io.decodeLines(decodeLine))
      .pipe(new stream.Transform({
        objectMode: true,
        
        transform(rec, enc, cb) {
          cb(null, [rec.key, rec[def.label], JSON.stringify(rec)]);
        }
      }))
      .pipe(pgu.encodePGText())
      .pipe(cp.stdin)
      .on('finish', () => ok())
      .on('error', fail);
  });

  return resP;
}

for (let name of Object.keys(imports)) {
  module.exports[name] = doImport.bind(null, name);
}
