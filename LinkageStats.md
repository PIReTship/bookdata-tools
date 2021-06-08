---
jupyter:
  jupytext:
    formats: ipynb,md
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.3'
      jupytext_version: 1.11.2
  kernelspec:
    display_name: Python 3
    language: python
    name: python3
---

# Book Data Linkage Statistics

This notebook presents statistics of the book data integration.


## Setup

```python
import pandas as pd
import seaborn as sns
import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy as np
```

## Compute Link Stats

Let's compute linkage statistics from our data!

We are first going to define our gender codes.  We'll start with the resolved codes:

```python
link_codes = ['female', 'male', 'ambiguous', 'unknown']
```

We want the unlink codes in order, so the last is the first link failure:

```python
unlink_codes = ['no-author-rec', 'no-book-author', 'no-book']
```

Now we can load gender data:

```python
viaf_genders = pd.read_parquet('book-links/cluster-genders.parquet')
viaf_genders = viaf_genders.set_index('cluster')['gender']
viaf_genders = viaf_genders.astype('category')
viaf_genders.cat.add_categories('no-book', inplace=True)
viaf_genders.cat.reorder_categories(link_codes + unlink_codes, inplace=True)
viaf_genders.describe()
```

```python
isbn_clusters = pd.read_parquet('book-links/isbn-clusters.parquet', columns=['isbn_id', 'cluster'])
isbn_clusters = isbn_clusters.set_index('isbn_id')['cluster']
isbn_clusters
```

Define a variable to store all of these:

```python
source_stats = {}
```

### LOC book corpus

```python
loc_isbns = pd.read_parquet('loc-mds/book-isbn-ids.parquet')
loc_clusters = loc_isbns.join(isbn_clusters, on='isbn_id')
loc_clusters
```

```python
loc_genders = loc_clusters.join(viaf_genders, on='cluster', how='left')
loc_genders
```

```python
loc_stats = loc_genders.groupby('gender')['rec_id'].count()
loc_stats = loc_stats.to_frame('n_books')
loc_stats
```

```python
source_stats['LOC-MDS'] = loc_stats
```

### BookCrossing

We want to process action genders.  Each action frame will have an 'item' column that we use - let's define a helper function:

```python
def action_stats(df):
    joined = df.join(viaf_genders, on='item', how='left')
    joined['gender'].fillna('no-book', inplace=True)
    return joined.groupby('gender')['item'].agg(['nunique', 'count']).rename(columns={
        'nunique': 'n_books',
        'count': 'n_actions'
    })
```

Do the implicit actions:

```python
bx_actions = pd.read_parquet('bx/bx-cluster-actions.parquet')
bx_actions.info()
```

```python
bx_act_stats = action_stats(bx_actions)
bx_act_stats
```

```python
source_stats['BX-I'] = bx_act_stats
```

And now we do ratings:

```python
bx_ratings = pd.read_parquet('bx/bx-cluster-ratings.parquet')
bx_ratings.info()
```

```python
bx_rate_stats = action_stats(bx_ratings)
bx_rate_stats
```

```python
source_stats['BX-E'] = bx_rate_stats
```

### Amazon data

Let's process the Amazon data:

```python
az_ratings = pd.read_parquet('az2014/az-cluster-ratings.parquet', columns=['user', 'item'])
az_ratings.info()
```

```python
az_stats = action_stats(az_ratings)
az_stats
```

```python
source_stats['AZ'] = az_stats
```

### GoodReads

Finally, we will load the GoodReads data.  First the ratings:

```python
gr_ratings = pd.read_parquet('goodreads/gr-cluster-ratings.parquet', columns=['user', 'item'])
gr_ratings.info()
```

```python
gr_rate_stats = action_stats(gr_ratings)
gr_rate_stats
```

```python
source_stats['GR-E'] = gr_rate_stats
del gr_ratings
```

And now the actions:

```python
gr_actions = pd.read_parquet('goodreads/gr-cluster-actions.parquet', columns=['user', 'item'])
gr_actions.info()
```

```python
gr_act_stats = action_stats(gr_actions)
gr_act_stats
```

```python
source_stats['GR-I'] = gr_act_stats
del gr_actions
```

### Integrating Statistics

Time to integrate all of these:

```python
link_stats = pd.concat(source_stats, names=['dataset'])
link_stats
```

Now we'll pivot each of our count columns into a table for easier reference.

```python
book_counts = link_stats['n_books'].unstack()
book_counts.sort_index(inplace=True)
book_counts
```

```python
act_counts = link_stats['n_actions'].unstack()
act_counts.drop(index='LOC-MDS', inplace=True)
act_counts.sort_index(inplace=True)
act_counts
```

We're going to want to compute versions of this table as fractions, e.g. the fraction of books that are written by women.  We will use the following helper function:

```python
def fractionalize(data, columns, unlinked=None):
    fracs = data[columns]
    fracs.columns = fracs.columns.astype('str')
    if unlinked:
        fracs = fracs.assign(unlinked=data[unlinked].sum(axis=1))
    totals = fracs.sum(axis=1)
    return fracs.divide(totals, axis=0)
```

And a helper function for plotting bar charts:

```python
def plot_bars(fracs, ax=None, cmap=mpl.cm.Dark2):
    if ax is None:
        ax = plt.gca()
    size = 0.5
    ind = np.arange(len(fracs))
    start = pd.Series(0, index=fracs.index)
    for i, col in enumerate(fracs.columns):
        vals = fracs.iloc[:, i]
        rects = ax.barh(ind, vals, size, left=start, label=col, color=cmap(i))
        for j, rec in enumerate(rects):
            if vals.iloc[j] < 0.1 or np.isnan(vals.iloc[j]): continue
            y = rec.get_y() + rec.get_height() / 2
            x = start.iloc[j] + vals.iloc[j] / 2
            ax.annotate('{:.1f}%'.format(vals.iloc[j] * 100),
                        xy=(x,y), ha='center', va='center', color='white',
                        fontweight='bold')
        start += vals.fillna(0)
    ax.set_xlabel('Fraction of Books')
    ax.set_ylabel('Data Set')
    ax.set_yticks(ind)
    ax.set_yticklabels(fracs.index)
    ax.legend(loc='center left', bbox_to_anchor=(1, 0.5))
```

## Resolution of Books

What fraction of *unique books* are resolved from each source?

```python
fractionalize(book_counts, link_codes + unlink_codes)
```

```python
plot_bars(fractionalize(book_counts, link_codes + unlink_codes))
```

```python
fractionalize(book_counts, link_codes, unlink_codes)
```

```python
plot_bars(fractionalize(book_counts, link_codes, unlink_codes))
```

```python
plot_bars(fractionalize(book_counts, ['female', 'male']))
```

## Resolution of Ratings

What fraction of *rating actions* have each resolution result?

```python
fractionalize(act_counts, link_codes + unlink_codes)
```

```python
plot_bars(fractionalize(act_counts, link_codes + unlink_codes))
```

```python
fractionalize(act_counts, link_codes, unlink_codes)
```

```python
plot_bars(fractionalize(act_counts, link_codes, unlink_codes))
```

```python
plot_bars(fractionalize(act_counts, ['female', 'male']))
```

```python

```
