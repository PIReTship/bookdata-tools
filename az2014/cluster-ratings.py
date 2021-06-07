"""
Aggregate Amazon ratings by cluster.
"""

import pandas as pd
from pandas.core.frame import DataFrame
import pyarrow as pa
import pyarrow.parquet as pq

from bookdata import script_log

_log = script_log("cluster-ratings")

_log.info('reading ISBNs')
clusters = pd.read_parquet("../book-links/isbn-clusters.parquet", columns=["isbn", "cluster"])
clusters = clusters.set_index('isbn')['cluster'].sort_index()

_log.info('reading ratings')
ratings = pd.read_parquet('ratings.parquet')

_log.info('merging ISBNs')
joined = ratings.join(clusters, on='asin')
assert len(joined) == len(ratings)

_log.info('aggregating by cluster')
groups = joined.groupby(['user', 'cluster'])
agg = pd.DataFrame({
    'rating': groups['rating'].agg('median'),
    # FIXME we need better aggregation
    'timestamp': groups['timestamp'].agg('max'),
})
agg = agg.reset_index().rename(columns={'cluster': 'item'})

_log.info('sorting final ratings')
agg.sort_values('timestamp', inplace=True, ignore_index=True)

_log.info('writing')
table = pa.Table.from_pandas(agg, preserve_index=False)
_log.info('raw table:\n%s', table)
schema = pa.schema([
    pa.field('user', pa.int32(), False),
    pa.field('item', pa.int32(), True),
    pa.field('rating', pa.float32(), False),
    pa.field('timestamp', pa.time64(), False),
])
table = table.cast(schema)
_log.info('converted table:\n%s', table)
pq.write_table(table, 'az-cluster-ratings.parquet', compression='zstd')
