"use strict";
const through = require('through2');

function decodeLines() {
    var buffer = Buffer.alloc(16 * 1024);
    var used = 0;

    return through.obj(function(chunk, enc, callback) {
        // find new line
        var idx = chunk.indexOf('\n');
        while (idx >= 0) {
            let buf = chunk.slice(0, idx);
            if (used) {
                buf = Buffer.concat([buffer.slice(0, used), buf]);
                used = 0;
            }
            let ltab = buf.lastIndexOf('\t');
            if (ltab < 0) {
                return callback(new Error("no tab found in line"));
            }

            let data = buf.slice(ltab + 1).toString();
            let json = JSON.parse(data);
            if (idx < buf.length - 1) {
                buffer = buf.slice(idx + 1);
            }
            this.push(json);
            chunk = chunk.slice(idx + 1);
            idx = chunk.indexOf('\n');
        }

        if (chunk.length > 0) {
            if (used + chunk.length > buffer.length) {
                var nb = Buffer.alloc(Math.max(used + chunk.length, buffer.length * 2));
                buffer.copy(nb, 0, 0, used);
                buffer = nb;
            }
                
            chunk.copy(buffer, used);
            used += chunk.length;
        }

        callback();
    });
}

module.exports = decodeLines;