#!/bin/bash -ex

set -o xtrace
set -o errexit

# Handle architectures from cibuildwheel
CMAKE_OSX_ARCHITECTURES="x86_64"
if [[ "$ARCHFLAGS" == *"arm64"* ]]
then
  # Compile libbson with arm64 support
  CMAKE_OSX_ARCHITECTURES="arm64;x86_64"
  # Download pyarrow with arm64 support
  pip install --platform macosx_11_0_arm64 --no-deps --only-binary=:all: pyarrow
fi


./build_libbson
