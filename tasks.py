import sys
from pathlib import Path
import subprocess as sp
import os

from invoke import task

data_dir = Path('data')
tgt_dir = Path('target')
bin_dir = tgt_dir / 'release'


def pipeline(steps, outfile=None):
    last = sp.DEVNULL
    if outfile is not None:
        outfd = os.open(outfile, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o666)
    else:
        outfd = None
    
    procs = []
    for step in steps[:-1]:
        proc = sp.Popen(step, stdin=last, stdout=sp.PIPE)
        last = proc.stdout
        procs.append(proc)
    
    proc = sp.Popen(steps[-1], stdin=last, stdout=outfd)
    procs.append(proc)

    for p, s in zip(procs, steps):
        rc = p.wait()
        if rc != 0:
            print(f'{s[0]} exited with code {rc}', file=sys.stderr)
            raise RuntimeError('subprocess failed')


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

    pipeline([
        ['pv', infile],
        ['gunzip'],
        [bin_dir / 'parse-marc'],
        ['gzip']
    ], outfile=outfile)
    

@task(build)
def convert_ol_authors(c, date='2018-10-31', progress=True):
    infile = data_dir / f'ol_dump_authors_{date}.txt.gz'
    outfile = data_dir / f'ol_dump_authors_{date}.psql.gz'

    pipeline([
        ['pv', infile],
        ['gunzip'],
        [bin_dir / 'clean-openlib'],
        ['gzip']
    ], outfile=outfile)


@task(build)
def convert_ol_editions(c, date='2018-10-31', progress=True):
    infile = data_dir / f'ol_dump_editions_{date}.txt.gz'
    outfile = data_dir / f'ol_dump_editions_{date}.psql.gz'

    pipeline([
        ['pv', infile],
        ['gunzip'],
        [bin_dir / 'clean-openlib'],
        ['gzip']
    ], outfile=outfile)


@task(build)
def convert_ol_works(c, date='2018-10-31', progress=True):
    infile = data_dir / f'ol_dump_works_{date}.txt.gz'
    outfile = data_dir / f'ol_dump_works_{date}.psql.gz'

    pipeline([
        ['pv', infile],
        ['gunzip'],
        [bin_dir / 'clean-openlib'],
        ['gzip']
    ], outfile=outfile)


if __name__ == '__main__':
    import invoke.program
    program = invoke.program.Program()
    program.run()
