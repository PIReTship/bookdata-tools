from pathlib import Path
import subprocess as sp
import os

from invoke import task

data_dir = Path('data')
tgt_dir = Path('target')
bin_dir = tgt_dir / 'release'


@task
def build(c, debug=False):
    "Compile the Rust support executables"
    global bin_dir
    if debug:
        c.run('cargo build')
        bin_dir = tgt_dir / 'debug'
    else:
        c.run('cargo build --release')


@task(build)
def convert_viaf(c, date='20181104', progress=True):
    infile = data_dir / f'viaf-{date}-clusters-marc21.xml.gz'
    outfile = data_dir / f'viaf-{date}-clusters.psql.gz'
    
    pv = sp.Popen(['pv', infile], stdin=sp.DEVNULL, stdout=sp.PIPE)
    gz = sp.Popen(['gunzip'], stdin=pv.stdout, stdout=sp.PIPE)
    parse = sp.Popen([bin_dir / 'parse-marc'], stdin=gz.stdout, stdout=sp.PIPE)
    fno = os.open(outfile, os.O_WRONLY | os.O_CREAT | os.O_TRUNC)
    gzout = sp.Popen(['gzip'], stdin=parse.stdout, stdout=fno)
    gzout.wait()


if __name__ == '__main__':
    import invoke.program
    program = invoke.program.Program()
    program.run()
