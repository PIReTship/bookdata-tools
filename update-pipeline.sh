#!/bin/sh

set -e
echo "Compiling book tools" >&2
cargo build --release

echo "Re-rendering data pipeline" >&2
./target/release/bookdata pipeline render
