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
# # Book Data Linkage Statistics
#
# This notebook presents statistics of the book data integration.

# %% [markdown]
# ## Setup

# %%
import pandas as pd
import seaborn as sns
import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy as np

# %%
from bookdata.db import db_url

# %% [markdown]
# ## Load Link Stats
#
# The `integration_stats` table in the database contains link success statistics in our data.

# %%
link_stats = pd.read_sql_table('integration_stats', db_url())
link_stats.head()

# %% [markdown]
# Let's create lists for our different codes, in order, for later handling.  We'll start with the resolved codes:

# %%
link_codes = ['female', 'male', 'ambiguous', 'unknown']

# %% [markdown]
# We want the unlink codes in order, so the last is the first link failure:

# %%
unlink_codes = ['no-viaf-author', 'no-loc-author', 'no-book']

# %% [markdown]
# Now we'll pivot each of our count columns into a table for easier reference.

# %%
book_counts = link_stats.pivot(index='dataset', columns='gender', values='n_books')
book_counts = book_counts.reindex(columns=link_codes + unlink_codes)
book_counts

# %%
act_counts = link_stats.pivot(index='dataset', columns='gender', values='n_actions')
act_counts = act_counts.reindex(columns=link_codes + unlink_codes)
act_counts.drop(index='LOC-MDS', inplace=True)
act_counts


# %% [markdown]
# We're going to want to compute versions of this table as fractions, e.g. the fraction of books that are written by women.  We will use the following helper function:

# %%
def fractionalize(data, columns, unlinked=None):
    fracs = data[columns]
    if unlinked:
        fracs = fracs.assign(unlinked=data[unlinked].sum(axis=1))
    totals = fracs.sum(axis=1)
    return fracs.divide(totals, axis=0)


# %% [markdown]
# And a helper function for plotting bar charts:

# %%
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
            if vals.iloc[j] < 0.1: continue
            y = rec.get_y() + rec.get_height() / 2
            x = start.iloc[j] + vals.iloc[j] / 2
            ax.annotate('{:.1f}%'.format(vals.iloc[j] * 100),
                        xy=(x,y), ha='center', va='center', color='white',
                        fontweight='bold')
        start += vals
    ax.set_xlabel('Fraction of Books')
    ax.set_ylabel('Data Set')
    ax.set_yticks(ind)
    ax.set_yticklabels(fracs.index)
    ax.legend(loc='center left', bbox_to_anchor=(1, 0.5))


# %% [markdown]
# ## Resolution of Books
#
# What fraction of *unique books* are resolved from each source?

# %%
fractionalize(book_counts, link_codes + unlink_codes)

# %%
plot_bars(fractionalize(book_counts, link_codes + unlink_codes))

# %%
fractionalize(book_counts, link_codes, unlink_codes)

# %%
plot_bars(fractionalize(book_counts, link_codes, unlink_codes))

# %%
plot_bars(fractionalize(book_counts, ['female', 'male']))

# %% [markdown]
# ## Resolution of Ratings
#
# What fraction of *rating actions* have each resolution result?

# %%
fractionalize(act_counts, link_codes + unlink_codes)

# %%
plot_bars(fractionalize(act_counts, link_codes + unlink_codes))

# %%
fractionalize(act_counts, link_codes, unlink_codes)

# %%
plot_bars(fractionalize(act_counts, link_codes, unlink_codes))

# %%
plot_bars(fractionalize(act_counts, ['female', 'male']))

# %% [markdown]
# ## Metrics
#
# Finally, we're going to write coverage metrics.

# %%
book_tots = book_counts.sum(axis=1)
book_link = book_counts['male'] + book_counts['female'] + book_counts['ambiguous']
book_cover = book_link / book_tots
book_cover

# %%
book_cover.to_json('book-coverage.json')

# %%
