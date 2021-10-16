# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.13.0
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Book Clustering Statistics
#
# This notebook provides statistics on the results of our book clustering.

# %% [markdown]
# ## Setup

# %%
import pandas as pd
import seaborn as sns
import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy as np
import json

# %%
from bookdata.db import db_url

# %% [markdown]
# ## Load Data
#
# Let's start by getting our clusters and their statistics:

# %%
clusters = pd.read_sql_table('cluster_stats', db_url())
clusters.info()

# %%
clusters.set_index('cluster', inplace=True)

# %% [markdown]
# Describe the count columns for basic descriptive stats:

# %%
clusters.describe()

# %% [markdown]
# 75% of clusters only contain 2 ISBNs (probably -10 and -13) and one book. OpenLibrary also contributes to the largest number of clusters.

# %%
tot = clusters.sum(axis=1)
tot.head()

# %%
with open('book-links/cluster-stats.json', 'w') as csf:
    json.dump({
        'clusters': len(tot),
        'largest': tot.max()
    }, csf)

# %% [markdown]
# ## Clusters per Source
#
# How many clusters are connected to each source?

# %%
src_counts = pd.Series(dict(
    (c, np.sum(clusters[c] > 0)) for c in clusters.columns
))
src_counts

# %%
src_counts.plot.barh()
plt.xlabel('# of Clusters')
plt.show()

# %% [markdown]
# ## Distributions
#
# Let's look at the distributions of cluster sizes.

# %%
size_dist = pd.concat(dict(
    (c, clusters[c].value_counts()) for c in clusters.columns
), names=['RecType'])
size_dist.index.set_names(['RecType', 'RecCount'], inplace=True)
size_dist = size_dist.reset_index(name='Clusters')
size_dist.head()

# %%
for rt, data in size_dist.groupby('RecType'):
    plt.scatter(data['RecCount'], data['Clusters'], marker='1', label=rt)
plt.legend()
plt.xscale('log')
plt.yscale('log')

# %% [markdown]
# Looks mostly fine - we expect a lot of power laws - but the number of clusters with merged GoodReads works is concerning.

# %% [markdown]
# ## GoodReads Work Merging
#
# Why are GoodReads works merging? Let's look at those.

# %%
gr_big = clusters[clusters['gr_works'] > 1].sort_values('gr_works', ascending=False)
gr_big.info()

# %% [markdown]
# We have 6K of these clusters. What fraction of the GoodReads-affected clusters is this?

# %%
len(gr_big) / clusters['gr_books'].count()

# %% [markdown]
# Less than 1%. Not bad, but let's look.

# %%
gr_big.head()

# %% [markdown]
# What's that big cluster?

# %%
biggest = gr_big.index[0]
cluster_titles = pd.read_sql(f'''
    SELECT DISTINCT gr_work_id, work_title
    FROM gr.book_cluster
    JOIN gr.book_ids USING (gr_book_id)
    JOIN gr.work_title USING (gr_work_id)
    WHERE cluster = {biggest} AND work_title IS NOT NULL;
''', db_url())
cluster_titles

# %% [markdown]
# Let's also find out how many *ratings* are affected by this.

# %%
pd.read_sql('''
    SELECT SUM((gr_works > 1)::int), AVG((gr_works > 1)::int)
    FROM gr.add_action
    JOIN gr.cluster_stats ON (book_id = cluster)
''', db_url())

# %% [markdown]
# Almost 9% of our add-to-shelf actions are for such a book - that's disappointing.

# %%
