(function() {var implementors = {
"arrow2":[],
"fallible_streaming_iterator":[],
"parquet2":[["impl&lt;P:&nbsp;<a class=\"trait\" href=\"parquet2/read/trait.PageIterator.html\" title=\"trait parquet2::read::PageIterator\">PageIterator</a>&gt; <a class=\"trait\" href=\"parquet2/trait.FallibleStreamingIterator.html\" title=\"trait parquet2::FallibleStreamingIterator\">FallibleStreamingIterator</a> for <a class=\"struct\" href=\"parquet2/read/struct.Decompressor.html\" title=\"struct parquet2::read::Decompressor\">Decompressor</a>&lt;P&gt;"],["impl&lt;I&gt; <a class=\"trait\" href=\"parquet2/trait.FallibleStreamingIterator.html\" title=\"trait parquet2::FallibleStreamingIterator\">FallibleStreamingIterator</a> for <a class=\"struct\" href=\"parquet2/read/struct.BasicDecompressor.html\" title=\"struct parquet2::read::BasicDecompressor\">BasicDecompressor</a>&lt;I&gt;<span class=\"where fmt-newline\">where<br>&nbsp;&nbsp;&nbsp;&nbsp;I: <a class=\"trait\" href=\"https://doc.rust-lang.org/1.65.0/core/iter/traits/iterator/trait.Iterator.html\" title=\"trait core::iter::traits::iterator::Iterator\">Iterator</a>&lt;Item = <a class=\"type\" href=\"parquet2/error/type.Result.html\" title=\"type parquet2::error::Result\">Result</a>&lt;<a class=\"enum\" href=\"parquet2/page/enum.CompressedPage.html\" title=\"enum parquet2::page::CompressedPage\">CompressedPage</a>&gt;&gt;,</span>"],["impl&lt;I:&nbsp;<a class=\"trait\" href=\"https://doc.rust-lang.org/1.65.0/core/iter/traits/iterator/trait.Iterator.html\" title=\"trait core::iter::traits::iterator::Iterator\">Iterator</a>&lt;Item = <a class=\"type\" href=\"parquet2/error/type.Result.html\" title=\"type parquet2::error::Result\">Result</a>&lt;<a class=\"enum\" href=\"parquet2/page/enum.EncodedPage.html\" title=\"enum parquet2::page::EncodedPage\">EncodedPage</a>&gt;&gt;&gt; <a class=\"trait\" href=\"parquet2/trait.FallibleStreamingIterator.html\" title=\"trait parquet2::FallibleStreamingIterator\">FallibleStreamingIterator</a> for <a class=\"struct\" href=\"parquet2/write/struct.Compressor.html\" title=\"struct parquet2::write::Compressor\">Compressor</a>&lt;I&gt;"],["impl&lt;'a, V, E&gt; <a class=\"trait\" href=\"parquet2/trait.FallibleStreamingIterator.html\" title=\"trait parquet2::FallibleStreamingIterator\">FallibleStreamingIterator</a> for <a class=\"struct\" href=\"parquet2/write/struct.DynStreamingIterator.html\" title=\"struct parquet2::write::DynStreamingIterator\">DynStreamingIterator</a>&lt;'a, V, E&gt;"]],
"polars":[],
"streaming_decompression":[["impl&lt;I, O, F, E, II&gt; <a class=\"trait\" href=\"streaming_decompression/trait.FallibleStreamingIterator.html\" title=\"trait streaming_decompression::FallibleStreamingIterator\">FallibleStreamingIterator</a> for <a class=\"struct\" href=\"streaming_decompression/struct.Decompressor.html\" title=\"struct streaming_decompression::Decompressor\">Decompressor</a>&lt;I, O, F, E, II&gt;<span class=\"where fmt-newline\">where<br>&nbsp;&nbsp;&nbsp;&nbsp;I: <a class=\"trait\" href=\"streaming_decompression/trait.Compressed.html\" title=\"trait streaming_decompression::Compressed\">Compressed</a>,<br>&nbsp;&nbsp;&nbsp;&nbsp;O: <a class=\"trait\" href=\"streaming_decompression/trait.Decompressed.html\" title=\"trait streaming_decompression::Decompressed\">Decompressed</a>,<br>&nbsp;&nbsp;&nbsp;&nbsp;E: <a class=\"trait\" href=\"https://doc.rust-lang.org/1.65.0/core/error/trait.Error.html\" title=\"trait core::error::Error\">Error</a>,<br>&nbsp;&nbsp;&nbsp;&nbsp;II: <a class=\"trait\" href=\"https://doc.rust-lang.org/1.65.0/core/iter/traits/iterator/trait.Iterator.html\" title=\"trait core::iter::traits::iterator::Iterator\">Iterator</a>&lt;Item = <a class=\"enum\" href=\"https://doc.rust-lang.org/1.65.0/core/result/enum.Result.html\" title=\"enum core::result::Result\">Result</a>&lt;I, E&gt;&gt;,<br>&nbsp;&nbsp;&nbsp;&nbsp;F: <a class=\"trait\" href=\"https://doc.rust-lang.org/1.65.0/core/ops/function/trait.Fn.html\" title=\"trait core::ops::function::Fn\">Fn</a>(I, &amp;mut <a class=\"struct\" href=\"https://doc.rust-lang.org/1.65.0/alloc/vec/struct.Vec.html\" title=\"struct alloc::vec::Vec\">Vec</a>&lt;<a class=\"primitive\" href=\"https://doc.rust-lang.org/1.65.0/std/primitive.u8.html\">u8</a>&gt;) -&gt; <a class=\"enum\" href=\"https://doc.rust-lang.org/1.65.0/core/result/enum.Result.html\" title=\"enum core::result::Result\">Result</a>&lt;O, E&gt;,</span>"]]
};if (window.register_implementors) {window.register_implementors(implementors);} else {window.pending_implementors = implementors;}})()