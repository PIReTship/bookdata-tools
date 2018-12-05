import sys
from pathlib import Path
import subprocess as sp
import os

from invoke import task, Collection
import ratings, support, viaf, openlib, loc


ns = Collection()
ns.add_collection(support)
ns.add_collection(ratings)
ns.add_collection(viaf)
ns.add_collection(openlib)
ns.add_collection(loc)

if __name__ == '__main__':
    import invoke.program
    program = invoke.program.Program()
    program.run()
