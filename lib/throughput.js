function throughput(name) {
    return {
        start: process.hrtime(),
        nrecs: 0,

        advance: function() {
            this.nrecs += 1;
            if (this.nrecs % 10000 == 0) {
                this.print();
            }
        },

        print: function() {
            var now = process.hrtime(this.start);
            var ftime = now[0] + now[1] * 1.0e-9;
            console.info("%s: processed %d records in %ss (%srecs/s)",
                name, this.nrecs, ftime.toFixed(3), (this.nrecs / ftime).toFixed(0));
        }
    };
}

module.exports = throughput;