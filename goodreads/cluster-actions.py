"""
Set things up for cluster-based ratings.
"""

import logging
import sys

import pandas as pd

_log = logging.getLogger('cluster-ratings')
logging.basicConfig(stream=sys.stderr, level=logging.INFO)

_log.info('reading actions')
actions = pd.read_parquet('gr-interactions.parquet', columns=['user_id', 'cluster', 'rating', 'updated'])
actions.info()

_log.info('filtering actions')
actions = actions[actions['cluster'].notnull()].astype({'cluster': 'i4'})
actions.reset_index(inplace=True, drop=True)
actions.info()

_log.info('summarizing actions')
last_act = actions.groupby(['user_id', 'cluster'])['updated'].agg(['max', 'min', 'count'])
last_act = last_act.reset_index(['user_id', 'cluster'])
last_act.info()
last_act = last_act.rename(columns={
    'user_id': 'user',
    'cluster': 'item',
    'max': 'timestamp',
    'min': 'first_time',
    'count': 'nactions'
}).astype({
    'user': 'u4',
    'item': 'i4',
    'nactions': 'i4'
})
last_act.info()

_log.info('writing actions')
last_act.to_parquet('gr-cluster-actions.parquet', compression='zstd', index=False)
