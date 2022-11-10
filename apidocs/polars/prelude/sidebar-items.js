window.SIDEBAR_ITEMS = {"constant":[["IDX_DTYPE",""],["NULL",""],["NULL_DTYPE",""]],"enum":[["AggExpr",""],["AnyValue",""],["ArrowDataType","The set of supported logical types in this crate."],["ArrowTimeUnit","The time units defined in Arrow."],["CsvEncoding",""],["DataType",""],["Excluded",""],["Expr","Queries consists of multiple expressions."],["FillNullStrategy",""],["FunctionExpr",""],["GroupByMethod",""],["GroupsIndicator",""],["GroupsProxy",""],["JoinType",""],["LiteralValue",""],["LogicalPlan",""],["NullStrategy",""],["NullValues",""],["Operator",""],["ParallelStrategy",""],["ParquetCompression","Defines the compression settings for writing a parquet file."],["PolarsError",""],["QuantileInterpolOptions",""],["TakeIdx","One of the three arguments allowed in unchecked_take"],["TakeRandBranch2",""],["TakeRandBranch3",""],["TimeUnit",""],["UniqueKeepStrategy",""]],"fn":[["all","Selects all columns"],["all_exprs","Evaluate all the expressions with a bitwise and"],["any_exprs","Evaluate all the expressions with a bitwise or"],["apply_binary",""],["apply_multiple","Apply a function/closure over the groups of multiple columns. This should only be used in a groupby aggregation."],["argsort_by","Find the indexes that would sort these series in order of appearance. That means that the first `Series` will be used to determine the ordering until duplicates are found. Once duplicates are found, the next `Series` will be used and so on."],["avg","Find the mean of all the values in this Expression."],["binary_expr",""],["cast","Cast expression."],["coalesce","Folds the expressions from left to right keeping the first no null values."],["col","Create a Column Expression based on a column name."],["collect_all","Collect all `LazyFrame` computations."],["cols","Select multiple columns by name"],["concat","Concat multiple"],["concat_lst","Concat lists entries."],["count","Count expression"],["cov","Compute the covariance between two columns."],["date_range","Create a date range from a `start` and `stop` expression."],["datetime",""],["datetime_to_timestamp_ms",""],["datetime_to_timestamp_ns",""],["datetime_to_timestamp_us",""],["dtype_col","Select multiple columns by dtype."],["dtype_cols","Select multiple columns by dtype."],["duration",""],["first","First column in DataFrame"],["fmt_groupby_column",""],["fold_exprs","Accumulate over multiple columns horizontally / row wise."],["is_not_null","IsNotNull expression."],["is_null","IsNull expression"],["last","Last column in DataFrame"],["lit","Create a Literal Expression from `L`"],["map_binary","Apply a closure on the two columns that are evaluated from `Expr` a and `Expr` b."],["map_list_multiple","Apply a function/closure over multiple columns once the logical plan get executed."],["map_multiple","Apply a function/closure over multiple columns once the logical plan get executed."],["max","Find the maximum of all the values in this Expression."],["max_exprs","Get the the maximum value per row"],["mean","Find the mean of all the values in this Expression."],["median","Find the median of all the values in this Expression."],["merge_dtypes",""],["min","Find the minimum of all the values in this Expression."],["min_exprs","Get the the minimum value per row"],["not","Not expression."],["pearson_corr","Compute the pearson correlation between two columns."],["quantile","Find a specific quantile of all the values in this Expression."],["range","Create a range literal."],["repeat","Repeat a literal `value` `n` times."],["resolve_homedir",""],["sum","Sum all the values in this Expression."],["sum_exprs","Get the the sum of the values per row"],["ternary_expr",""],["when","Start a when-then-otherwise expression"]],"macro":[["df",""]],"mod":[["aggregations",""],["datatypes","Data types supported by Polars."],["default_arrays",""],["expr",""],["full",""],["groupby",""],["hash_join",""],["list",""],["predicates",""],["read_impl",""],["row",""],["utils",""],["zip",""]],"struct":[["AggregationContext",""],["AnonymousScanOptions",""],["Arc","A thread-safe reference-counting pointer. ‘Arc’ stands for ‘Atomically Reference Counted’."],["ArrowField","Represents Arrow’s metadata of a “column”."],["ArrowSchema","An ordered sequence of [`Field`]s with associated [`Metadata`]."],["BatchedParquetReader",""],["BoolTakeRandom",""],["BoolTakeRandomSingleChunk",""],["BooleanChunkedBuilder",""],["BooleanType",""],["BrotliLevel","Represents a valid brotli compression level."],["CategoricalType",""],["ChunkedArray","ChunkedArray"],["CsvReader","Create a new DataFrame by reading a csv file."],["CsvWriter","Write a DataFrame to csv."],["DataFrame","A contiguous growable collection of `Series` that have the same length."],["DateType",""],["DatetimeArgs",""],["DatetimeType",""],["DurationArgs",""],["DurationType",""],["Field","Characterizes the name and the [`DataType`] of a column."],["Float32Type",""],["Float64Type",""],["GroupBy","Returned by a groupby operation on a DataFrame. This struct supports several aggregations."],["GroupsIdx","Indexes of the groups, the first index is stored separately. this make sorting fast."],["GroupsProxyIter",""],["GroupsProxyParIter",""],["GzipLevel","Represents a valid gzip compression level."],["Int16Type",""],["Int32Type",""],["Int64Type",""],["Int8Type",""],["JoinBuilder",""],["JoinOptions",""],["LazyCsvReader",""],["LazyFrame","Lazy abstraction over an eager `DataFrame`. It really is an abstraction over a logical plan. The methods of this struct will incrementally modify a logical plan until output is requested (via collect)"],["LazyGroupBy","Utility struct for lazy groupby operation."],["ListBooleanChunkedBuilder",""],["ListNameSpace","Specialized expressions for [`Series`] of [`DataType::List`]."],["ListPrimitiveChunkedBuilder",""],["ListTakeRandom",""],["ListTakeRandomSingleChunk",""],["ListType",""],["ListUtf8ChunkedBuilder",""],["Logical","Maps a logical type to a a chunked array implementation of the physical type. This saves a lot of compiler bloat and allows us to reuse functionality."],["MeltArgs","Arguments for `[DataFrame::melt]` function"],["NoNull","Just a wrapper structure. Useful for certain impl specializations This is for instance use to implement `impl<T> FromIterator<T::Native> for NoNull<ChunkedArray<T>>` as `Option<T::Native>` was already implemented: `impl<T> FromIterator<Option<T::Native>> for ChunkedArray<T>`"],["Null","The literal Null"],["NumTakeRandomChunked",""],["NumTakeRandomCont",""],["NumTakeRandomSingleChunk",""],["OptState","State of the allowed optimizations"],["ParquetReader","Read Apache parquet format into a DataFrame."],["ParquetWriter","Write a DataFrame to parquet format"],["PhysicalIoHelper","Wrapper struct that allow us to use a PhysicalExpr in polars-io."],["PrimitiveChunkedBuilder",""],["RecordBatchIter",""],["ScanArgsAnonymous",""],["ScanArgsParquet",""],["Schema",""],["Series","Series"],["SlicedGroups",""],["SortOptions",""],["SpecialEq","Wrapper type that has special equality properties depending on the inner type specialization"],["StrpTimeOptions",""],["TakeRandomBitmap",""],["TimeType",""],["UInt16Type",""],["UInt32Type",""],["UInt64Type",""],["UInt8Type",""],["Utf8ChunkedBuilder",""],["Utf8TakeRandom",""],["Utf8TakeRandomSingleChunk",""],["Utf8Type",""],["When","Intermediate state of `when(..).then(..).otherwise(..)` expr."],["WhenThen","Intermediate state of `when(..).then(..).otherwise(..)` expr."],["WhenThenThen","Intermediate state of chain when then exprs."],["ZstdLevel","Represents a valid zstd compression level."]],"trait":[["AnonymousScan",""],["ArgAgg","Argmin/ Argmax"],["ArrowGetItem",""],["AsList",""],["AsUtf8",""],["BinaryUdfOutputField",""],["ChunkAgg","Aggregation operations"],["ChunkAggSeries","Aggregations that return Series of unit length. Those can be used in broadcasting operations."],["ChunkAnyValue",""],["ChunkApply","Fastest way to do elementwise operations on a ChunkedArray when the operation is cheaper than branching due to null checking"],["ChunkApplyKernel","Apply kernels on the arrow array chunks in a ChunkedArray."],["ChunkBytes",""],["ChunkCast","Cast `ChunkedArray<T>` to `ChunkedArray<N>`"],["ChunkCompare","Compare Series and ChunkedArray’s and get a `boolean` mask that can be used to filter rows."],["ChunkExpandAtIndex","Create a new ChunkedArray filled with values at that index."],["ChunkExplode","Explode/ flatten a List or Utf8 Series"],["ChunkFillNull","Replace None values with various strategies"],["ChunkFillNullValue","Replace None values with a value"],["ChunkFilter","Filter values by a boolean mask."],["ChunkFull","Fill a ChunkedArray with one value."],["ChunkFullNull",""],["ChunkPeaks","Find local minima/ maxima"],["ChunkQuantile","Quantile and median aggregation"],["ChunkReverse","Reverse a ChunkedArray"],["ChunkSet","Create a `ChunkedArray` with new values by index or by boolean mask. Note that these operations clone data. This is however the only way we can modify at mask or index level as the underlying Arrow arrays are immutable."],["ChunkShift",""],["ChunkShiftFill","Shift the values of a ChunkedArray by a number of periods."],["ChunkSort","Sort operations on `ChunkedArray`."],["ChunkTake","Fast access by index."],["ChunkTakeEvery","Traverse and collect every nth element"],["ChunkUnique","Get unique values in a `ChunkedArray`"],["ChunkVar","Variance and standard deviation aggregation."],["ChunkZip","Combine 2 ChunkedArrays based on some predicate."],["ChunkedBuilder",""],["ChunkedSet",""],["DataFrameOps",""],["FromData",""],["FromDataBinary",""],["FromDataUtf8",""],["FunctionOutputField",""],["IndexOfSchema","This trait exists to be unify the API of polars Schema and arrows Schema"],["IndexToUsize",""],["InitHashMaps",""],["IntoGroupsProxy","Used to create the tuples for a groupby operation."],["IntoLazy",""],["IntoListNameSpace",""],["IntoSeries","Used to convert a [`ChunkedArray`], `&dyn SeriesTrait` and [`Series`] into a [`Series`]."],["IntoSeriesOps",""],["IntoTakeRandom","Create a type that implements a faster `TakeRandom`."],["IntoVec",""],["IsFloat","Safety"],["LhsNumOps",""],["ListBuilderTrait",""],["ListFromIter",""],["ListNameSpaceExtension",""],["ListNameSpaceImpl",""],["Literal",""],["LogicalType",""],["MutableBitmapExtension",""],["NamedFrom",""],["NamedFromOwned",""],["NewChunkedArray",""],["NumOpsDispatch",""],["NumericNative",""],["PartitionedAggregation",""],["PhysicalExpr","Take a DataFrame and evaluate the expressions. Implement this for Column, lt, eq, etc"],["PolarsArray",""],["PolarsDataType",""],["PolarsFloatType",""],["PolarsIntegerType",""],["PolarsIterator","A `PolarsIterator` is an iterator over a `ChunkedArray` which contains polars types. A `PolarsIterator` must implement `ExactSizeIterator` and `DoubleEndedIterator`."],["PolarsNumericType",""],["PolarsSingleType","Any type that is not nested"],["QuantileAggSeries",""],["Range",""],["RenameAliasFn",""],["SerReader",""],["SerWriter",""],["SeriesBinaryUdf","A wrapper trait for any binary closure `Fn(Series, Series) -> PolarsResult<Series>`"],["SeriesMethods",""],["SeriesOps",""],["SeriesSealed",""],["SeriesTrait",""],["SeriesUdf","A wrapper trait for any closure `Fn(Vec<Series>) -> PolarsResult<Series>`"],["TakeIterator",""],["TakeIteratorNulls",""],["TakeRandom","Random access"],["TakeRandomUtf8",""],["UdfSchema",""],["ValueSize",""],["VarAggSeries",""],["VecHash",""]],"type":[["AllowedOptimizations","AllowedOptimizations"],["ArrayRef",""],["ArrowChunk",""],["BooleanChunked",""],["BorrowIdxItem",""],["DateChunked",""],["DatetimeChunked",""],["Dummy","Dummy type, we need to instantiate all generic types, so we fill one with a dummy."],["DurationChunked",""],["FillNullLimit",""],["Float32Chunked",""],["Float64Chunked",""],["GetOutput",""],["GroupsSlice","Every group is indicated by an array where the"],["IdxArr",""],["IdxCa",""],["IdxItem",""],["IdxSize","The type used by polars to index data."],["IdxType",""],["Int16Chunked",""],["Int32Chunked",""],["Int64Chunked",""],["Int8Chunked",""],["LargeBinaryArray",""],["LargeListArray",""],["LargeStringArray",""],["ListChunked",""],["PlHashMap",""],["PlHashSet",""],["PlIndexMap",""],["PlIndexSet",""],["PolarsResult",""],["SchemaRef",""],["TimeChunked",""],["TimeZone",""],["UInt16Chunked",""],["UInt32Chunked",""],["UInt64Chunked",""],["UInt8Chunked",""],["Utf8Chunked",""]]};