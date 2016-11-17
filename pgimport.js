"use strict";

const zlib = require('zlib');
const fs = require('fs');
const through = require('through2');
const fws = require('flush-write-stream');
const decodeLines = require('./lib/decode');
const pg = require('pg');
const async = require('async');
const throughput = require('./lib/throughput');

const options = require('yargs').argv;

var ninserts = 0;

var autp = throughput('authors');
var wtp = throughput('works');
var etp = throughput('editions');

const imports = {
    authors: function (done) {
        fs.createReadStream('data/ol_dump_authors_2016-07-31.txt.gz')
            .pipe(zlib.createUnzip())
            .pipe(decodeLines())
            .pipe(fws.obj((rec, enc, cb) => {
                autp.advance();
                client.query('INSERT INTO authors (author_key, author_name, author_data) VALUES ($1, $2, $3)',
                            [rec.key, rec.name, JSON.stringify(rec)],
                            (err) => {
                                if (err) return cb(err);
                                ninserts += 1;
                                if (ninserts % 10000 == 0) {
                                    async.series([
                                        (cb) => client.query('COMMIT', cb),
                                        (cb) => client.query('BEGIN ISOLATION LEVEL READ UNCOMMITTED', cb)
                                    ], cb);
                                } else {
                                    cb();
                                }
                            });
            }), (cb) => {
                console.info("finished authors");
                cb();
            });
    },
    works: function (done) {
        fs.createReadStream('data/ol_dump_works_2016-07-31.txt.gz')
            .pipe(zlib.createUnzip())
            .pipe(decodeLines())
            .pipe(fws.obj((rec, enc, cb) => {
                wtp.advance();
                var actions = [
                    (cb) => client.query('INSERT INTO works (work_key, work_title, work_data) VALUES ($1, $2, $3)',
                                        [rec.key, rec.title, JSON.stringify(rec)], cb)
                ];
                if (rec.authors) {
                    for (var au of rec.authors) {
                        if (au.author) {
                            actions.push((cb) => {
                                client.query('INSERT INTO work_authors_tmp (work_key, author_key) VALUES ($1, $2)',
                                            [rec.key, au.author.key], cb);
                            });
                        }
                    }
                }
                async.series(actions, (err) => {
                    if (err) return cb(err);
                    ninserts += 1;
                    if (ninserts % 10000 == 0) {
                        async.series([
                            (cb) => client.query('COMMIT', cb),
                            (cb) => client.query('BEGIN ISOLATION LEVEL READ UNCOMMITTED', cb)
                        ], cb);
                    } else {
                        cb();
                    }
                });
            }), (cb) => {
                console.info("finished works");
                cb();
            });
    },
    editions: function(done) {
        fs.createReadStream('data/ol_dump_editions_2016-07-31.txt.gz')
            .pipe(zlib.createUnzip())
            .pipe(decodeLines())
            .pipe(fws.obj((rec, enc, cb) => {
                etp.advance();
                var actions = [
                    (cb) => client.query('INSERT INTO editions (edition_key, edition_title, edition_data) VALUES ($1, $2, $3)',
                                        [rec.key, rec.title, JSON.stringify(rec)], cb)
                ];
                if (rec.works) {
                    for (var w of rec.works) {
                        actions.push((cb) => {
                            client.query('INSERT INTO edition_works_tmp (edition_key, work_key) VALUES ($1, $2)',
                                        [rec.key, w.key], cb);
                        });
                    }
                }
                if (rec.authors) {
                    for (var au of rec.authors) {
                        if (au.author) {
                            actions.push((cb) => {
                                client.query('INSERT INTO edition_authors_tmp (edition_key, author_key) VALUES ($1, $2)',
                                            [rec.key, au.author.key], cb);
                            });
                        }
                    }
                }
                async.series(actions, (err) => {
                    if (err) return cb(err);
                    ninserts += 1;
                    if (ninserts % 10000 == 0) {
                        async.series([
                            (cb) => client.query('COMMIT', cb),
                            (cb) => client.query('BEGIN ISOLATION LEVEL READ UNCOMMITTED', cb)
                        ], cb);
                    } else {
                        cb();
                    }
                });
            }), (cb) => {
                console.info("finished editions");
                cb();
            });
    }
};

const client = new pg.Client();

client.connect(function(err) {
    if (err) throw err;
    
    async.series([
        (cb) => client.query('BEGIN ISOLATION LEVEL READ UNCOMMITTED', cb),
        (cb) => {
            async.parallel(options._.map((k) => imports[k], cb))
        },
        (cb) => {
            client.query('COMMIT', cb)
        }
    ], (err) => {
        console.info("finished processing, now done");
        client.end((e2) => {
            if (err) throw err;
            if (e2) throw e2;
        });
    });
});