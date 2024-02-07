(function() {var type_impls = {
"bookdata":[["<details class=\"toggle implementors-toggle\" open><summary><section id=\"impl-IdIndex%3CK%3E\" class=\"impl\"><a class=\"src rightside\" href=\"src/bookdata/ids/index.rs.html#34-103\">source</a><a href=\"#impl-IdIndex%3CK%3E\" class=\"anchor\">§</a><h3 class=\"code-header\">impl&lt;K&gt; <a class=\"struct\" href=\"bookdata/ids/index/struct.IdIndex.html\" title=\"struct bookdata::ids::index::IdIndex\">IdIndex</a>&lt;K&gt;<div class=\"where\">where\n    K: <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/cmp/trait.Eq.html\" title=\"trait core::cmp::Eq\">Eq</a> + <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/hash/trait.Hash.html\" title=\"trait core::hash::Hash\">Hash</a>,</div></h3></section></summary><div class=\"impl-items\"><details class=\"toggle method-toggle\" open><summary><section id=\"method.new\" class=\"method\"><a class=\"src rightside\" href=\"src/bookdata/ids/index.rs.html#39-44\">source</a><h4 class=\"code-header\">pub fn <a href=\"bookdata/ids/index/struct.IdIndex.html#tymethod.new\" class=\"fn\">new</a>() -&gt; <a class=\"struct\" href=\"bookdata/ids/index/struct.IdIndex.html\" title=\"struct bookdata::ids::index::IdIndex\">IdIndex</a>&lt;K&gt;</h4></section></summary><div class=\"docblock\"><p>Create a new index.</p>\n</div></details><details class=\"toggle method-toggle\" open><summary><section id=\"method.freeze\" class=\"method\"><a class=\"src rightside\" href=\"src/bookdata/ids/index.rs.html#48-53\">source</a><h4 class=\"code-header\">pub fn <a href=\"bookdata/ids/index/struct.IdIndex.html#tymethod.freeze\" class=\"fn\">freeze</a>(self) -&gt; <a class=\"struct\" href=\"bookdata/ids/index/struct.IdIndex.html\" title=\"struct bookdata::ids::index::IdIndex\">IdIndex</a>&lt;K&gt;</h4></section></summary><div class=\"docblock\"><p>Freeze the index so no new items can be added.</p>\n</div></details><details class=\"toggle method-toggle\" open><summary><section id=\"method.len\" class=\"method\"><a class=\"src rightside\" href=\"src/bookdata/ids/index.rs.html#56-58\">source</a><h4 class=\"code-header\">pub fn <a href=\"bookdata/ids/index/struct.IdIndex.html#tymethod.len\" class=\"fn\">len</a>(&amp;self) -&gt; <a class=\"primitive\" href=\"https://doc.rust-lang.org/nightly/std/primitive.usize.html\">usize</a></h4></section></summary><div class=\"docblock\"><p>Get the index length</p>\n</div></details><details class=\"toggle method-toggle\" open><summary><section id=\"method.intern\" class=\"method\"><a class=\"src rightside\" href=\"src/bookdata/ids/index.rs.html#61-76\">source</a><h4 class=\"code-header\">pub fn <a href=\"bookdata/ids/index/struct.IdIndex.html#tymethod.intern\" class=\"fn\">intern</a>&lt;Q&gt;(&amp;mut self, key: <a class=\"primitive\" href=\"https://doc.rust-lang.org/nightly/std/primitive.reference.html\">&amp;Q</a>) -&gt; <a class=\"type\" href=\"bookdata/prelude/type.Result.html\" title=\"type bookdata::prelude::Result\">Result</a>&lt;<a class=\"primitive\" href=\"https://doc.rust-lang.org/nightly/std/primitive.i32.html\">i32</a>, <a class=\"enum\" href=\"bookdata/ids/index/enum.IndexError.html\" title=\"enum bookdata::ids::index::IndexError\">IndexError</a>&gt;<div class=\"where\">where\n    K: <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/borrow/trait.Borrow.html\" title=\"trait core::borrow::Borrow\">Borrow</a>&lt;Q&gt;,\n    Q: <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/hash/trait.Hash.html\" title=\"trait core::hash::Hash\">Hash</a> + <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/cmp/trait.Eq.html\" title=\"trait core::cmp::Eq\">Eq</a> + <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/alloc/borrow/trait.ToOwned.html\" title=\"trait alloc::borrow::ToOwned\">ToOwned</a>&lt;Owned = K&gt; + ?<a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/marker/trait.Sized.html\" title=\"trait core::marker::Sized\">Sized</a>,</div></h4></section></summary><div class=\"docblock\"><p>Get the ID for a key, adding it to the index if needed.</p>\n</div></details><details class=\"toggle method-toggle\" open><summary><section id=\"method.intern_owned\" class=\"method\"><a class=\"src rightside\" href=\"src/bookdata/ids/index.rs.html#79-86\">source</a><h4 class=\"code-header\">pub fn <a href=\"bookdata/ids/index/struct.IdIndex.html#tymethod.intern_owned\" class=\"fn\">intern_owned</a>(&amp;mut self, key: K) -&gt; <a class=\"type\" href=\"bookdata/prelude/type.Result.html\" title=\"type bookdata::prelude::Result\">Result</a>&lt;<a class=\"primitive\" href=\"https://doc.rust-lang.org/nightly/std/primitive.i32.html\">i32</a>, <a class=\"enum\" href=\"bookdata/ids/index/enum.IndexError.html\" title=\"enum bookdata::ids::index::IndexError\">IndexError</a>&gt;</h4></section></summary><div class=\"docblock\"><p>Get the ID for a key, adding it to the index if needed and transferring ownership.</p>\n</div></details><details class=\"toggle method-toggle\" open><summary><section id=\"method.lookup\" class=\"method\"><a class=\"src rightside\" href=\"src/bookdata/ids/index.rs.html#90-96\">source</a><h4 class=\"code-header\">pub fn <a href=\"bookdata/ids/index/struct.IdIndex.html#tymethod.lookup\" class=\"fn\">lookup</a>&lt;Q&gt;(&amp;self, key: <a class=\"primitive\" href=\"https://doc.rust-lang.org/nightly/std/primitive.reference.html\">&amp;Q</a>) -&gt; <a class=\"enum\" href=\"https://doc.rust-lang.org/nightly/core/option/enum.Option.html\" title=\"enum core::option::Option\">Option</a>&lt;<a class=\"primitive\" href=\"https://doc.rust-lang.org/nightly/std/primitive.i32.html\">i32</a>&gt;<div class=\"where\">where\n    K: <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/borrow/trait.Borrow.html\" title=\"trait core::borrow::Borrow\">Borrow</a>&lt;Q&gt;,\n    Q: <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/hash/trait.Hash.html\" title=\"trait core::hash::Hash\">Hash</a> + <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/cmp/trait.Eq.html\" title=\"trait core::cmp::Eq\">Eq</a> + ?<a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/marker/trait.Sized.html\" title=\"trait core::marker::Sized\">Sized</a>,</div></h4></section></summary><div class=\"docblock\"><p>Look up the ID for a key if it is present.</p>\n</div></details><details class=\"toggle method-toggle\" open><summary><section id=\"method.keys\" class=\"method\"><a class=\"src rightside\" href=\"src/bookdata/ids/index.rs.html#100-102\">source</a><h4 class=\"code-header\">pub fn <a href=\"bookdata/ids/index/struct.IdIndex.html#tymethod.keys\" class=\"fn\">keys</a>(&amp;self) -&gt; <a class=\"struct\" href=\"https://docs.rs/hashbrown/0.14.3/hashbrown/map/struct.Keys.html\" title=\"struct hashbrown::map::Keys\">Keys</a>&lt;'_, K, <a class=\"primitive\" href=\"https://doc.rust-lang.org/nightly/std/primitive.i32.html\">i32</a>&gt;</h4></section></summary><div class=\"docblock\"><p>Iterate over keys (see <a href=\"https://doc.rust-lang.org/nightly/std/collections/hash/map/struct.HashMap.html#method.keys\" title=\"method std::collections::hash::map::HashMap::keys\">std::collections::HashMap::keys</a>).</p>\n</div></details></div></details>",0,"bookdata::goodreads::users::UserIndex"],["<details class=\"toggle implementors-toggle\" open><summary><section id=\"impl-IdIndex%3CString%3E\" class=\"impl\"><a class=\"src rightside\" href=\"src/bookdata/ids/index.rs.html#105-211\">source</a><a href=\"#impl-IdIndex%3CString%3E\" class=\"anchor\">§</a><h3 class=\"code-header\">impl <a class=\"struct\" href=\"bookdata/ids/index/struct.IdIndex.html\" title=\"struct bookdata::ids::index::IdIndex\">IdIndex</a>&lt;<a class=\"struct\" href=\"https://doc.rust-lang.org/nightly/alloc/string/struct.String.html\" title=\"struct alloc::string::String\">String</a>&gt;</h3></section></summary><div class=\"impl-items\"><details class=\"toggle method-toggle\" open><summary><section id=\"method.key_vec\" class=\"method\"><a class=\"src rightside\" href=\"src/bookdata/ids/index.rs.html#107-118\">source</a><h4 class=\"code-header\">pub fn <a href=\"bookdata/ids/index/struct.IdIndex.html#tymethod.key_vec\" class=\"fn\">key_vec</a>(&amp;self) -&gt; <a class=\"struct\" href=\"https://doc.rust-lang.org/nightly/alloc/vec/struct.Vec.html\" title=\"struct alloc::vec::Vec\">Vec</a>&lt;&amp;<a class=\"primitive\" href=\"https://doc.rust-lang.org/nightly/std/primitive.str.html\">str</a>&gt;</h4></section></summary><div class=\"docblock\"><p>Get the keys in order.</p>\n</div></details><details class=\"toggle method-toggle\" open><summary><section id=\"method.data_frame\" class=\"method\"><a class=\"src rightside\" href=\"src/bookdata/ids/index.rs.html#121-129\">source</a><h4 class=\"code-header\">pub fn <a href=\"bookdata/ids/index/struct.IdIndex.html#tymethod.data_frame\" class=\"fn\">data_frame</a>(\n    &amp;self,\n    id_col: &amp;<a class=\"primitive\" href=\"https://doc.rust-lang.org/nightly/std/primitive.str.html\">str</a>,\n    key_col: &amp;<a class=\"primitive\" href=\"https://doc.rust-lang.org/nightly/std/primitive.str.html\">str</a>\n) -&gt; <a class=\"type\" href=\"bookdata/prelude/type.Result.html\" title=\"type bookdata::prelude::Result\">Result</a>&lt;<a class=\"struct\" href=\"https://docs.rs/polars-core/0.37.0/polars_core/frame/struct.DataFrame.html\" title=\"struct polars_core::frame::DataFrame\">DataFrame</a>, PolarsError&gt;</h4></section></summary><div class=\"docblock\"><p>Conver this ID index into a <a href=\"https://docs.rs/polars-core/0.37.0/polars_core/frame/struct.DataFrame.html\" title=\"struct polars_core::frame::DataFrame\">DataFrame</a>, with columns for ID and key.</p>\n</div></details><details class=\"toggle method-toggle\" open><summary><section id=\"method.load_standard\" class=\"method\"><a class=\"src rightside\" href=\"src/bookdata/ids/index.rs.html#137-139\">source</a><h4 class=\"code-header\">pub fn <a href=\"bookdata/ids/index/struct.IdIndex.html#tymethod.load_standard\" class=\"fn\">load_standard</a>&lt;P: <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/convert/trait.AsRef.html\" title=\"trait core::convert::AsRef\">AsRef</a>&lt;<a class=\"struct\" href=\"bookdata/prelude/struct.Path.html\" title=\"struct bookdata::prelude::Path\">Path</a>&gt;&gt;(path: P) -&gt; <a class=\"type\" href=\"bookdata/prelude/type.Result.html\" title=\"type bookdata::prelude::Result\">Result</a>&lt;<a class=\"struct\" href=\"bookdata/ids/index/struct.IdIndex.html\" title=\"struct bookdata::ids::index::IdIndex\">IdIndex</a>&lt;<a class=\"struct\" href=\"https://doc.rust-lang.org/nightly/alloc/string/struct.String.html\" title=\"struct alloc::string::String\">String</a>&gt;&gt;</h4></section></summary><div class=\"docblock\"><p>Load from a Parquet file, with a standard configuration.</p>\n<p>This assumes the Parquet file has the following columns:</p>\n<ul>\n<li><code>key</code>, of type <code>String</code>, storing the keys</li>\n<li><code>id</code>, of type <code>i32</code>, storing the IDs</li>\n</ul>\n</div></details><details class=\"toggle method-toggle\" open><summary><section id=\"method.load\" class=\"method\"><a class=\"src rightside\" href=\"src/bookdata/ids/index.rs.html#146-169\">source</a><h4 class=\"code-header\">pub fn <a href=\"bookdata/ids/index/struct.IdIndex.html#tymethod.load\" class=\"fn\">load</a>&lt;P: <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/convert/trait.AsRef.html\" title=\"trait core::convert::AsRef\">AsRef</a>&lt;<a class=\"struct\" href=\"bookdata/prelude/struct.Path.html\" title=\"struct bookdata::prelude::Path\">Path</a>&gt;&gt;(\n    path: P,\n    id_col: &amp;<a class=\"primitive\" href=\"https://doc.rust-lang.org/nightly/std/primitive.str.html\">str</a>,\n    key_col: &amp;<a class=\"primitive\" href=\"https://doc.rust-lang.org/nightly/std/primitive.str.html\">str</a>\n) -&gt; <a class=\"type\" href=\"bookdata/prelude/type.Result.html\" title=\"type bookdata::prelude::Result\">Result</a>&lt;<a class=\"struct\" href=\"bookdata/ids/index/struct.IdIndex.html\" title=\"struct bookdata::ids::index::IdIndex\">IdIndex</a>&lt;<a class=\"struct\" href=\"https://doc.rust-lang.org/nightly/alloc/string/struct.String.html\" title=\"struct alloc::string::String\">String</a>&gt;&gt;</h4></section></summary><div class=\"docblock\"><p>Load from a Parquet file.</p>\n<p>This loads two columns from a Parquet file.  The ID column is expected to\nhave type <code>UInt32</code> (or a type projectable to it), and the key column should\nbe <code>Utf8</code>.</p>\n</div></details><details class=\"toggle method-toggle\" open><summary><section id=\"method.load_csv\" class=\"method\"><a class=\"src rightside\" href=\"src/bookdata/ids/index.rs.html#176-192\">source</a><h4 class=\"code-header\">pub fn <a href=\"bookdata/ids/index/struct.IdIndex.html#tymethod.load_csv\" class=\"fn\">load_csv</a>&lt;P: <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/convert/trait.AsRef.html\" title=\"trait core::convert::AsRef\">AsRef</a>&lt;<a class=\"struct\" href=\"bookdata/prelude/struct.Path.html\" title=\"struct bookdata::prelude::Path\">Path</a>&gt;, K: <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/cmp/trait.Eq.html\" title=\"trait core::cmp::Eq\">Eq</a> + <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/hash/trait.Hash.html\" title=\"trait core::hash::Hash\">Hash</a> + <a class=\"trait\" href=\"https://docs.rs/serde/1.0.196/serde/de/trait.DeserializeOwned.html\" title=\"trait serde::de::DeserializeOwned\">DeserializeOwned</a>&gt;(\n    path: P\n) -&gt; <a class=\"type\" href=\"bookdata/prelude/type.Result.html\" title=\"type bookdata::prelude::Result\">Result</a>&lt;<a class=\"struct\" href=\"bookdata/ids/index/struct.IdIndex.html\" title=\"struct bookdata::ids::index::IdIndex\">IdIndex</a>&lt;K&gt;&gt;</h4></section></summary><div class=\"docblock\"><p>Load an index from a CSV file.</p>\n<p>This loads an index from a CSV file.  It assumes the first column is the ID, and the\nsecond column is the key.</p>\n</div></details><details class=\"toggle method-toggle\" open><summary><section id=\"method.save_standard\" class=\"method\"><a class=\"src rightside\" href=\"src/bookdata/ids/index.rs.html#195-197\">source</a><h4 class=\"code-header\">pub fn <a href=\"bookdata/ids/index/struct.IdIndex.html#tymethod.save_standard\" class=\"fn\">save_standard</a>&lt;P: <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/convert/trait.AsRef.html\" title=\"trait core::convert::AsRef\">AsRef</a>&lt;<a class=\"struct\" href=\"bookdata/prelude/struct.Path.html\" title=\"struct bookdata::prelude::Path\">Path</a>&gt;&gt;(&amp;self, path: P) -&gt; <a class=\"type\" href=\"bookdata/prelude/type.Result.html\" title=\"type bookdata::prelude::Result\">Result</a>&lt;<a class=\"primitive\" href=\"https://doc.rust-lang.org/nightly/std/primitive.unit.html\">()</a>&gt;</h4></section></summary><div class=\"docblock\"><p>Save to a Parquet file with the standard configuration.</p>\n</div></details><details class=\"toggle method-toggle\" open><summary><section id=\"method.save\" class=\"method\"><a class=\"src rightside\" href=\"src/bookdata/ids/index.rs.html#200-210\">source</a><h4 class=\"code-header\">pub fn <a href=\"bookdata/ids/index/struct.IdIndex.html#tymethod.save\" class=\"fn\">save</a>&lt;P: <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/convert/trait.AsRef.html\" title=\"trait core::convert::AsRef\">AsRef</a>&lt;<a class=\"struct\" href=\"bookdata/prelude/struct.Path.html\" title=\"struct bookdata::prelude::Path\">Path</a>&gt;&gt;(\n    &amp;self,\n    path: P,\n    id_col: &amp;<a class=\"primitive\" href=\"https://doc.rust-lang.org/nightly/std/primitive.str.html\">str</a>,\n    key_col: &amp;<a class=\"primitive\" href=\"https://doc.rust-lang.org/nightly/std/primitive.str.html\">str</a>\n) -&gt; <a class=\"type\" href=\"bookdata/prelude/type.Result.html\" title=\"type bookdata::prelude::Result\">Result</a>&lt;<a class=\"primitive\" href=\"https://doc.rust-lang.org/nightly/std/primitive.unit.html\">()</a>&gt;</h4></section></summary><div class=\"docblock\"><p>Save to a Parquet file with the standard configuration.</p>\n</div></details></div></details>",0,"bookdata::goodreads::users::UserIndex"]]
};if (window.register_type_impls) {window.register_type_impls(type_impls);} else {window.pending_type_impls = type_impls;}})()