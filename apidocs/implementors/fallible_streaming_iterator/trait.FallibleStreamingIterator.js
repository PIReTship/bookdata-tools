(function() {var implementors = {
"fallible_streaming_iterator":[],
"polars_parquet":[["impl&lt;I: <a class=\"trait\" href=\"https://doc.rust-lang.org/1.74.0/core/iter/traits/iterator/trait.Iterator.html\" title=\"trait core::iter::traits::iterator::Iterator\">Iterator</a>&lt;Item = <a class=\"type\" href=\"polars_parquet/parquet/error/type.Result.html\" title=\"type polars_parquet::parquet::error::Result\">Result</a>&lt;<a class=\"enum\" href=\"polars_parquet/parquet/page/enum.Page.html\" title=\"enum polars_parquet::parquet::page::Page\">Page</a>&gt;&gt;&gt; <a class=\"trait\" href=\"polars_parquet/parquet/trait.FallibleStreamingIterator.html\" title=\"trait polars_parquet::parquet::FallibleStreamingIterator\">FallibleStreamingIterator</a> for <a class=\"struct\" href=\"polars_parquet/parquet/write/struct.Compressor.html\" title=\"struct polars_parquet::parquet::write::Compressor\">Compressor</a>&lt;I&gt;"],["impl&lt;I&gt; <a class=\"trait\" href=\"polars_parquet/parquet/trait.FallibleStreamingIterator.html\" title=\"trait polars_parquet::parquet::FallibleStreamingIterator\">FallibleStreamingIterator</a> for <a class=\"struct\" href=\"polars_parquet/parquet/read/struct.BasicDecompressor.html\" title=\"struct polars_parquet::parquet::read::BasicDecompressor\">BasicDecompressor</a>&lt;I&gt;<span class=\"where fmt-newline\">where\n    I: <a class=\"trait\" href=\"https://doc.rust-lang.org/1.74.0/core/iter/traits/iterator/trait.Iterator.html\" title=\"trait core::iter::traits::iterator::Iterator\">Iterator</a>&lt;Item = <a class=\"type\" href=\"polars_parquet/parquet/error/type.Result.html\" title=\"type polars_parquet::parquet::error::Result\">Result</a>&lt;<a class=\"enum\" href=\"polars_parquet/parquet/page/enum.CompressedPage.html\" title=\"enum polars_parquet::parquet::page::CompressedPage\">CompressedPage</a>&gt;&gt;,</span>"],["impl&lt;'a, V, E&gt; <a class=\"trait\" href=\"polars_parquet/parquet/trait.FallibleStreamingIterator.html\" title=\"trait polars_parquet::parquet::FallibleStreamingIterator\">FallibleStreamingIterator</a> for <a class=\"struct\" href=\"polars_parquet/parquet/write/struct.DynStreamingIterator.html\" title=\"struct polars_parquet::parquet::write::DynStreamingIterator\">DynStreamingIterator</a>&lt;'a, V, E&gt;"],["impl&lt;P: <a class=\"trait\" href=\"polars_parquet/parquet/read/trait.PageIterator.html\" title=\"trait polars_parquet::parquet::read::PageIterator\">PageIterator</a>&gt; <a class=\"trait\" href=\"polars_parquet/parquet/trait.FallibleStreamingIterator.html\" title=\"trait polars_parquet::parquet::FallibleStreamingIterator\">FallibleStreamingIterator</a> for <a class=\"struct\" href=\"polars_parquet/parquet/read/struct.Decompressor.html\" title=\"struct polars_parquet::parquet::read::Decompressor\">Decompressor</a>&lt;P&gt;"]],
"streaming_decompression":[["impl&lt;I, O, F, E, II&gt; <a class=\"trait\" href=\"streaming_decompression/trait.FallibleStreamingIterator.html\" title=\"trait streaming_decompression::FallibleStreamingIterator\">FallibleStreamingIterator</a> for <a class=\"struct\" href=\"streaming_decompression/struct.Decompressor.html\" title=\"struct streaming_decompression::Decompressor\">Decompressor</a>&lt;I, O, F, E, II&gt;<span class=\"where fmt-newline\">where\n    I: <a class=\"trait\" href=\"streaming_decompression/trait.Compressed.html\" title=\"trait streaming_decompression::Compressed\">Compressed</a>,\n    O: <a class=\"trait\" href=\"streaming_decompression/trait.Decompressed.html\" title=\"trait streaming_decompression::Decompressed\">Decompressed</a>,\n    E: <a class=\"trait\" href=\"https://doc.rust-lang.org/1.74.0/core/error/trait.Error.html\" title=\"trait core::error::Error\">Error</a>,\n    II: <a class=\"trait\" href=\"https://doc.rust-lang.org/1.74.0/core/iter/traits/iterator/trait.Iterator.html\" title=\"trait core::iter::traits::iterator::Iterator\">Iterator</a>&lt;Item = <a class=\"enum\" href=\"https://doc.rust-lang.org/1.74.0/core/result/enum.Result.html\" title=\"enum core::result::Result\">Result</a>&lt;I, E&gt;&gt;,\n    F: <a class=\"trait\" href=\"https://doc.rust-lang.org/1.74.0/core/ops/function/trait.Fn.html\" title=\"trait core::ops::function::Fn\">Fn</a>(I, &amp;mut <a class=\"struct\" href=\"https://doc.rust-lang.org/1.74.0/alloc/vec/struct.Vec.html\" title=\"struct alloc::vec::Vec\">Vec</a>&lt;<a class=\"primitive\" href=\"https://doc.rust-lang.org/1.74.0/std/primitive.u8.html\">u8</a>&gt;) -&gt; <a class=\"enum\" href=\"https://doc.rust-lang.org/1.74.0/core/result/enum.Result.html\" title=\"enum core::result::Result\">Result</a>&lt;O, E&gt;,</span>"]]
};if (window.register_implementors) {window.register_implementors(implementors);} else {window.pending_implementors = implementors;}})()