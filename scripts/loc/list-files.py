"""
Usage:
    scripts.loc.list-files URL DIR
"""

import requests
import re
from pathlib import Path
import html5lib

from bookdata import script_log
from docopt import docopt

_log = script_log(__name__)

args = docopt(__doc__)

url = args.get('URL')
out_dir = Path(args.get('DIR'))

_log.info('fetching %s', url)
res = requests.get(url)

tree = html5lib.parse(res.text)
url_re = re.compile(r'^(?P<name>[A-Za-z.]+)\.(?P<year>\d+)\.part(?P<part>\d+)\.xml\.gz$')

links = {}

for link in tree.findall('.//{http://www.w3.org/1999/xhtml}a'):
    href = link.get('href')
    _log.debug('checking link %s', href)
    lm = url_re.match(href)
    if lm is not None:
        file = lm['name']
        year = lm['year']
        fn = lm[0]
        key = f'{file}.{year}'
        if key not in links:
            links[key] = []
        links[key].append(fn)

out_dir.mkdir(exist_ok=True, parents=True)
for key, files in links.items():
    _log.info('writing %d files for %s', len(files), key)
    kf = out_dir / f'{key}.lst'
    with kf.open('w') as f:
        for fn in files:
            f.write(f'{url}{fn}\n')
