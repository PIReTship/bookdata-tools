const miss = require('mississippi');
const util = require('util');

function throughput(name, bs) {
    if (!bs) bs = 10000;
    process.stderr.write(name + ':');

    return {
        start: process.hrtime(),
        nrecs: 0,

        advance: function() {
            this.nrecs += 1;
            if (this.nrecs % bs == 0) {
                this.print();
            }
        },

        print: function(final) {
            var now = process.hrtime(this.start);
            var ftime = now[0] + now[1] * 1.0e-9;
            process.stderr.write("\r%s: processed %d records in %ss (%s recs/s)",
                name, this.nrecs, ftime.toFixed(3), (this.nrecs / ftime).toFixed(0));
           if (final) {
               process.stderr.write('\n');
           }
        }
    };
}

module.exports = throughput;
module.exports.stream = function(name, bs) {
    let tp = throughput(name, bs);
    return miss.through.obj((chunk, enc, cb) => {
        tp.advance();
        cb(null, chunk);
    }, (cb) => {
        tp.print(true);
        cb();
    });
};
