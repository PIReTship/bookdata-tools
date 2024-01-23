# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.14.0
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %% [markdown]
# # First Books
#
# This notebook prepares a data set of book information for a prediction task to try to predict if a new author will publish a second book.  Michael Ekstrand uses it for teaching data science.

# %% [markdown]
# ## Setup

# %%
import polars as pl
import matplotlib.pyplot as plt
import seaborn as sns

# %% [markdown]
# ## Book Statistics
#
# The first step is to compute some book interaction statistics.
#
# Let's load and link:

# %%
links = pl.scan_parquet('gr-book-ids.parquet')
ixs = pl.scan_parquet('gr-interactions.parquet')
ixs = ixs.join(links, on='book_id')

# %% [markdown]
# Now aggregate into statistics:

# %%
work_stats = ixs.groupby('work_id').agg([
    # number of add-to-shelf actions
    pl.col('rec_id').count().alias('n_shelves'),
    # number of distinct users who interact with it
    pl.col('user_id').n_unique().alias('n_users'),
    # number of ratings
    pl.col('rating').where(pl.col('rating') > 0).count().alias('n_rates'),
    # mean rating
    pl.col('rating').where(pl.col('rating') > 0).mean().alias('mean_rate'),
    # number of "positive" ratings
    (pl.col('rating') > 2).sum().alias('n_pos_rates'),
])

# %% [markdown]
# Link with work year:

# %%
book_info = pl.scan_parquet('gr-book-info.parquet')
book_link = pl.scan_parquet('gr-book-link.parquet')
book_info = book_info.join(book_link, on='book_id')
work_year = book_info.groupby('work_id').agg(pl.col('pub_year').min())
work_stats = work_stats.join(work_year, on='work_id')

# %% [markdown]
# Now a bit of a detour - we need authors. Let's load those:

# %%
book_authors = pl.scan_parquet('gr-book-authors.parquet')

# %% [markdown]
# And we want to get the *first* work of each author:

# %%
author_works = book_authors.join(book_info, on='book_id').filter(pl.col('pub_year').is_not_null()).sort([
    'pub_year',
    'pub_month',
    'pub_date',
])
author_first_work = author_works.groupby('author_id').agg(pl.col('work_id').first())

# %% [markdown]
# Now we only want authors first works that were published since GoodReads started in 2007, and no later than 2012 to give the author time to have a new book before the data runs out in 2017:

# %%
first_work_stats = work_stats.join(author_first_work, on='work_id')
first_work_stats = first_work_stats.filter((pl.col('pub_year') >= 2008) & (pl.col('pub_year') <= 2012))

# %% [markdown]
# Ok - now we have a table of first-work statistics.  We're going to take those authors, and find out how many total works they have in the data set.

# %%
author_nworks = first_work_stats.select(['author_id']).join(author_works, on='author_id').groupby('author_id').agg([
    pl.col('work_id').n_unique().alias('au_nbooks')
])

# %% [markdown]
# Join this with the original table:

# %%
mb_table = first_work_stats.join(author_nworks, on='author_id')

# %% [markdown]
# Finally, we can compute this entire table. How will Polars do it?

# %%
print(mb_table.describe_optimized_plan())

# %% [markdown]
# And run it:

# %%
mb_table = mb_table.collect()
mb_table

# %%
mb_table.write_csv('author-first-works.csv')

# %% [markdown]
# ## Author Names
#
# Let's get author names and work titles for some debugging info.

# %%
authors = pl.read_parquet('gr-author-info.parquet')

# %%
authors = authors.join(mb_table.select('author_id'), on='author_id')

# %%
authors.write_csv('afw-author-names.csv')

# %%
works = pl.read_parquet('gr-work-info.parquet')
works = works.join(mb_table.select('work_id'), on='work_id')
works.write_csv('afw-work-titles.csv')

# %%
