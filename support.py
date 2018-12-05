from pathlib import Path
import subprocess as sp
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
