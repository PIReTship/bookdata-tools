import sys
import logging
from hashlib import md5
import pandas as pd


def cluster_hash(s):
    hd = str(s.name)
    hd = hd + '|' + s.str.cat(sep='|')
    h = md5(hd.encode('utf8'))
    return h.hexdigest()


_log = logging.getLogger('cluster-hash')
logging.basicConfig(level=logging.INFO, stream=sys.stderr)

_log.info('reading ISBNs')
clusters = pd.read_parquet('isbn-clusters.parquet', columns=['cluster', 'isbn'])

_log.info('sorting ISBNs')
clusters.sort_values(['cluster', 'isbn'], ignore_index=True, inplace=True)

_log.info('computing hashes')
hashes = clusters.groupby('cluster')['isbn'].apply(cluster_hash)
hashes = hashes.to_frame('isbn_hash').reset_index()
print(hashes)
hashes.to_parquet('cluster-hashes.parquet', index=False, compression='zstd')
