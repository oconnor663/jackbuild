#! /usr/bin/env bash

set -e -u -o pipefail

HERE="$(realpath "$(dirname "$BASH_SOURCE")")"

set -x

cd "$HERE/../rust_lib"
cargo zigbuild --target=x86_64-pc-windows-gnu

cd "$(mktemp -d)"
pwd
cp "$HERE/main.c" .
mkdir c_lib
cp "$HERE/../c_lib/lib.h" c_lib/
mkdir rust_lib
cbindgen --lang=c "$HERE/../rust_lib" > rust_lib/lib.h
zig cc -luserenv -lws2_32 -lunwind --target=x86_64-windows-gnu \
    main.c \
    "$HERE/../c_lib/lib.c" \
    "$HERE/../rust_lib/target/x86_64-pc-windows-gnu/debug/librust_lib.a"
./a.exe
