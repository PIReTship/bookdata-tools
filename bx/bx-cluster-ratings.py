from numpy import dtype
import polars as pl

isbns = pl.scan_parquet("../book-links/isbn-clusters.parquet")
isbns = isbns.select(['isbn', 'cluster'])

ratings = pl.scan_csv('cleaned-ratings.csv')
ratings = ratings.select([
    pl.col('user').cast(pl.Int32),
    pl.col('isbn'),
    pl.col('rating').cast(pl.Float32),
]).filter(pl.col('rating') > pl.lit(0))

joined = ratings.join(isbns, on='isbn')
joined = joined.select(['user', 'cluster', 'rating'])

actions = joined.groupby(['user', 'cluster']).agg([
    pl.col('cluster').count().alias('nactions'),
    pl.col('rating').median(),
])
actions = actions.collect()

actions.write_parquet('bx-cluster-ratings.parquet', compression='zstd')
