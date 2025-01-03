#! /usr/bin/env bash

set -e -u -o pipefail

HERE="$(realpath "$(dirname "$BASH_SOURCE")")"

set -x

cd "$HERE/../rust_lib"
cargo build

cd "$(mktemp -d)"
pwd
cp "$HERE/main.c" .
mkdir c_lib
cp "$HERE/../c_lib/lib.h" c_lib/
mkdir rust_lib
cbindgen --lang=c "$HERE/../rust_lib" > rust_lib/lib.h
gcc \
    main.c \
    "$HERE/../c_lib/lib.c" \
    "$HERE/../rust_lib/target/debug/librust_lib.a"
./a.out
