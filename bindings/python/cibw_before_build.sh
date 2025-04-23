#!/bin/bash -ex

set -o xtrace
set -o errexit

# Handle architectures from cibuildwheel.
# Set CMAKE_OSX_ARCHITECTURES for libbson.
# Get the appropriate version of pyarrow for macos.

if [[ "$CIBW_BUILD" == *"macosx_"* ]]
then
  if [[ "$ARCHFLAGS" == *"arm64"* ]]
  then
    platform="macosx_12_0_arm64"
    export CMAKE_OSX_ARCHITECTURES="arm64"
  else
    platform="macosx_12_0_x86_64"
    export CMAKE_OSX_ARCHITECTURES="x86_64"
  fi

  # Install pyarrow with the appropriate platform.
  pip install --platform $platform --upgrade --target $HOME/wheels --no-deps --only-binary=:all: pyarrow
fi

# Install just uv, needed for the build command.
pip install rust-just uv

# Build libbson with the appropriate arch.
CMAKE_BUILD_TYPE=Release just build-libbson
