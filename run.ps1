# Small helper script to build and run book data tools.

cargo run --release -- $args
if (-not $?) {
    exit 1
}
