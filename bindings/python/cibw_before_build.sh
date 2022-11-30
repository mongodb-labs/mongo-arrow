#!/bin/bash -ex

set -o xtrace
set -o errexit

# Handle architectures from cibuildwheel.
# Set CMAKE_OSX_ARCHITECTURES for libbson.
# Get the appropriate version of pyarrow for macos.

if [[ "$CIBW_BUILD" == *"macosx_"* ]]
then
  mac_version="${MACOSX_DEPLOYMENT_TARGET/\./_}"
  if [[ "$ARCHFLAGS" == *"arm64"* ]]
  then
    platform="macosx_${mac_version}_arm64"
    export CMAKE_OSX_ARCHITECTURES="arm64"
  else
    platform="macosx_${mac_version}_x86_64"
  fi

  # Install pyarrow with the appropriate platform.
  pip install --platform $platform --upgrade --target $HOME/wheels --no-deps --only-binary=:all: pyarrow
fi

# Build libbson with the appropriate arch.
CMAKE_BUILD_TYPE=Release ./build-libbson.sh
