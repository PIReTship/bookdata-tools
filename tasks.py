import sys
from pathlib import Path
import subprocess as sp
import os

import numpy as np
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


@task
def init_viaf(c):
    "Initialize the VIAF schema"
    print('initializing VIAF schema')
    c.run('psql -f viaf-schema.sql')


@task(build, init_viaf)
def import_viaf(c, date='20181104', progress=True):
    "Import VIAF data"
    infile = data_dir / f'viaf-{date}-clusters-marc21.xml.gz'
    print('importing VIAF data from %s', infile)

    pipeline([
        [bin_dir / 'parse-marc', infile],
        ['psql', '-c', '\\copy viaf_marc_field FROM STDIN']
    ])


@task
def init_ol(c):
    "Initialize the OpenLibrary schema"
    print('initializing OpenLibrary schema')
    c.run('psql -f ol-schema.sql')


@task(build)
def import_ol_authors(c, date='2018-10-31', progress=True):
    infile = data_dir / f'ol_dump_authors_{date}.txt.gz'

    pipeline([
        [bin_dir / 'clean-openlib', infile],
        ['psql', '-c', '\\copy ol_author (author_key, author_data) FROM STDIN']
    ])


@task(build)
def import_ol_editions(c, date='2018-10-31', progress=True):
    infile = data_dir / f'ol_dump_editions_{date}.txt.gz'

    pipeline([
        [bin_dir / 'clean-openlib', infile],
        ['psql', '-c', '\\copy ol_edition (edition_key, edition_data) FROM STDIN']
    ])


@task(build)
def import_ol_works(c, date='2018-10-31', progress=True):
    infile = data_dir / f'ol_dump_works_{date}.txt.gz'

    pipeline([
        [bin_dir / 'clean-openlib', infile],
        ['psql', '-c', '\\copy ol_work (work_key, work_data) FROM STDIN']
    ])


@task
def import_bx_ratings(c):
    "Import BookCrossing ratings"
    print("initializing BX schema")
    c.run('psql -f bx-schema.sql')
    print("cleaning BX rating data")
    with open('data/BX-Book-Ratings.csv', 'rb') as bf:
        data = bf.read()
    barr = np.frombuffer(data, dtype='u1')
    # delete bytes that are too big
    barr = barr[barr < 128]
    # convert to LF
    barr = barr[barr != ord('\r')]
    # change delimiter to tab
    barr[barr == ord(';')] = ord('\t')

    # write
    print('importing BX to database')
    data = bytes(barr)
    psql = sp.Popen(['psql', '-c', '\\copy bx_raw_ratings FROM STDIN'],
                    stdin=sp.PIPE)
    psql.stdin.write(data)
    rc = psql.wait()
    if rc:
        raise RuntimeError('psql exited with code %d', rc)

if __name__ == '__main__':
    import invoke.program
    program = invoke.program.Program()
    program.run()
