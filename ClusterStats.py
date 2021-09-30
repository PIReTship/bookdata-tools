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

# %% [markdown]
# ## Load Data
#
# Let's start by getting our clusters and their statistics:

# %%
clusters = pd.read_parquet('book-links/cluster-stats.parquet')
clusters.info()

# %%
clusters.set_index('cluster', inplace=True)

# %% [markdown]
# Describe the count columns for basic descriptive stats:

# %%
clusters.describe()

# %% [markdown]
# 75% of clusters only contain 2 ISBNs (probably -10 and -13) and one book. OpenLibrary also contributes to the largest number of clusters.

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
plt.xlabel('# of Records')
plt.xscale('log')
plt.ylabel('# of Clusters')
plt.yscale('log')
plt.show()

# %% [markdown]
# Looks mostly fine - we expect a lot of power laws - but the number of clusters with merged GoodReads works is concerning.

# %% [markdown]
# ## GoodReads Work Merging
#
# Why are GoodReads works merging? Let's look at those.

# %%
gr_big = clusters[clusters['n_gr_works'] > 1].sort_values('n_gr_works', ascending=False)
gr_big.info()

# %% [markdown]
# We have 6K of these clusters. What fraction of the GoodReads-affected clusters is this?

# %%
len(gr_big) / clusters['n_gr_books'].count()

# %% [markdown]
# Less than 1%. Not bad, but let's look.

# %%
gr_big.head()

# %% [markdown]
# ## Output Cluster Statistics
#
# Let's compute some cluster statistics and show them.

# %%
import json
with open('book-links/cluster-stats.json', 'w') as sf:
    json.dump({
        'clusters': len(clusters)
    }, sf)
