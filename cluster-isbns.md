
# ISBN Clustering


```python
import pandas as pd
import numpy as np
from numba import njit
```


```python
%matplotlib inline
```


```python
db_url = 'postgresql://openlib:piratelib@localhost/openlib'
```


```python
numspaces = dict(work=100000000, edition=200000000, rec=300000000, isbn=900000000)
```

## Clustering Algorithm

We cluster ISBNs by taking the bipartite graph of ISBNs and records, and computing the closure for each ISBN.  Each closure becomes a cluster with a single ‘book’ ID.


```python
@njit
def _make_clusters(clusters, ls, rs):
    iters = 0
    nchanged = len(ls)
    
    while nchanged > 0:
        nchanged = 0
        iters = iters + 1
        for i in range(len(ls)):
            left = ls[i]
            right = rs[i]
            if clusters[left] < clusters[right]:
                clusters[right] = clusters[left]
                nchanged += 1
                
    return iters
```


```python
def cluster_isbns(isbn_recs):
    print('initializing isbn vector')
    isbns = isbn_recs.groupby('isbn_id').record.min()
    isbns = isbns.reset_index(name='cluster')
    isbns['ino'] = np.arange(len(isbns), dtype=np.int32)
    intbl = pd.merge(isbn_recs, isbns.loc[:, ['isbn_id', 'ino']])
    left = intbl.loc[:, ['record', 'ino']].rename(columns={'ino': 'left'})
    right = intbl.loc[:, ['record', 'ino']].rename(columns={'ino': 'right'})
    print('making edge table')
    edges = pd.merge(left, right)
    print('clustering')
    iters = _make_clusters(isbns.cluster.values, edges.left.values, edges.right.values)
    print('clustered in', iters, 'iterations')
    return isbns
```


```python
def plot_cluster_sizes(clusters):
    cluster_sizes = clusters.groupby('cluster').isbn_id.count()
    size_acc = cluster_sizes.reset_index(name='size').groupby('size').cluster.count()
    size_acc = size_acc.reset_index(name='nclusters')
    return size_acc.plot.scatter(x='size', y='nclusters', loglog=True)
```

## Library of Congress


```python
loc_rec_isbns = pd.read_sql('''
SELECT isbn_id, rec_id AS record
FROM loc_rec_isbn
''', db_url)
```


```python
loc_rec_isbns.head()
```


```python
loc_clusters = cluster_isbns(loc_rec_isbns)
```


```python
plot_cluster_sizes(loc_clusters)
```


```python
loc_clusters.to_csv('data/loc-clusters.csv', index=False, header=False)
```

## OpenLibrary


```python
ol_rec_edges = pd.read_sql('''
SELECT isbn_id, book_code AS record
FROM ol_isbn_link
''', db_url)
```


```python
ol_clusters = cluster_isbns(ol_rec_edges)
```


```python
plot_cluster_sizes(ol_clusters)
```


```python
ol_clusters.to_csv('data/ol-clusters.csv', index=False, header=False)
```

## Integrated Clusters


```python
all_isbn_recs = pd.concat([
    loc_rec_isbns.assign(record=lambda df: df.record + numspaces['rec']),
    ol_rec_edges
])
```


```python
int_clusters = cluster_isbns(all_isbn_recs)
```


```python
plot_cluster_sizes(int_clusters)
```


```python
int_clusters.to_csv('data/isbn-clusters.csv', index=False, header=False)
```
