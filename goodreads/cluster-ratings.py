"""
Set things up for cluster-based ratings.
"""

import logging
import sys

import pandas as pd

_log = logging.getLogger('cluster-ratings')
logging.basicConfig(stream=sys.stderr, level=logging.INFO)

_log.info('reading actions')
actions = pd.read_parquet('gr-interactions.parquet', columns=['user_id', 'book_id', 'rating'])
actions.info()

_log.info('reading book links')
links = pd.read_parquet('gr-book-link.parquet', use_nullable_dtypes=True)

clusters = links[links['cluster'].notnull()]
clusters = clusters[['book_id', 'cluster']].astype('uint32')
clusters = clusters.set_index('book_id').sort_index()
clusters.info()

_log.info('joining cluster data')
cract = actions.join(clusters, on='book_id', how='inner')

_log.info('computing median ratings')
crates = cract[cract.rating.notnull()]
crates = crates.groupby(['user_id', 'cluster'])['rating'].median()
crates = crates.to_frame().reset_index()
_log.info('writing %d ratings', len(crates))
crates.to_parquet('gr-cluster-ratings.parquet', index=False, compression='zstd')
_log.info('finished ratings')
del crates
