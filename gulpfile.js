const gulp = require('gulp');
const gutil = require('gulp-util');

const pgimport = require('./pgimport');

exports.importAuthors = pgimport.authors;
exports.importWorks = pgimport.works;
exports.importEditions = pgimport.editions;

exports.importOpenLib = gulp.parallel(
  exports.importAuthors,
  exports.importWorks,
  exports.importEditions
);
exports.importOpenLib.description = 'Import all OpenLib data';
