#!/bin/bash -ex

set -o xtrace
set -o errexit

# Handle architectures from cibuildwheel.
# Set CMAKE_OSX_ARCHITECTURES for libbson.

if [[ "$CIBW_BUILD" == *"macosx_"* ]]
then
  if [[ "$ARCHFLAGS" == *"arm64"* ]]
  then
    export CMAKE_OSX_ARCHITECTURES="arm64"
  else
    export CMAKE_OSX_ARCHITECTURES="x86_64"
  fi
fi

# Install just uv, needed for the build command.
pip install rust-just uv

# Build libbson with the appropriate arch.
CMAKE_BUILD_TYPE=Release just build-libbson
