import sys
from dvc.main import main
from docopt import docopt
import yaml


def clean_yaml(args):
    file = args[0]
    with open(file, 'r') as rf:
        obj = yaml.load(rf, Loader=yaml.CSafeLoader)
    if 'md5' in obj:
        del obj['md5']
    if 'deps' in obj:
        for dep in obj['deps']:
            if 'md5' in dep:
                del dep['md5']
    if 'outs' in obj:
        for out in obj['outs']:
            if 'md5' in out:
                del out['md5']
    yaml.dump(obj, sys.stdout)


if __name__ == '__main__':
    if sys.argv[1] == '_clean_yaml':
        clean_yaml(sys.argv[2:])
    else:
        try:
            from bookdata import dvcpatch
            dvcpatch.patch()
        except ImportError:
            pass
        main()
