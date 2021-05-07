"""
Set things up for cluster-based ratings.
"""

import logging
import sys

import numpy as np
import pandas as pd

_log = logging.getLogger('cluster-ratings')
logging.basicConfig(stream=sys.stderr, level=logging.INFO)

_log.info('reading actions')
actions = pd.read_parquet('gr-interactions.parquet', columns=['user_id', 'cluster', 'updated'])
actions.info()

_log.info('filtering actions')
actions = actions[actions['cluster'].notnull()].astype({'cluster': 'i4'})
actions.reset_index(inplace=True, drop=True)
actions.info()

_log.info('finding unique actions')
dups = actions.duplicated(['user_id', 'cluster'], False)
singles = actions[~dups].reset_index(drop=True).rename(columns={
    'user_id': 'user',
    'cluster': 'item',
    'updated': 'timestamp'
})
singles['first_time'] = singles['timestamp']
singles['nactions'] = 1

_log.info('summarizing non-unique actions')
duped = actions[dups].reset_index(drop=True)
last_act = duped.groupby(['user_id', 'cluster'])['updated'].agg(['max', 'min', 'count'])
last_act = last_act.reset_index()
last_act.info()
last_act = last_act.rename(columns={
    'user_id': 'user',
    'cluster': 'item',
    'max': 'timestamp',
    'min': 'first_time',
    'count': 'nactions'
})
last_act.info()

_log.info('combining actions')
result = pd.concat([singles, last_act], ignore_index=True)
result = result.astype({
    'user': 'i4',
    'item': 'i4',
    'nactions': 'i4'
})
result.info()

_log.info('writing actions')
result.to_parquet('gr-cluster-actions.parquet', compression='zstd', index=False)
