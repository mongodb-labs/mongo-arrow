#!/bin/bash -ex

set -o xtrace
set -o errexit

# Handle architectures from cibuildwheel.
# Set CMAKE_OSX_ARCHITECTURES for libbson.
# Get the appropriate platoform for pip to download pyarrow.

mac_version="${MACOSX_DEPLOYMENT_TARGET/\./_}"

if [[ "$CIBW_BUILD" == *"macosx_"* ]]
then
  os_name=macosx
elif [[ "$CIBW_BUILD" == *"win_amd64"* ]]
then
  os_name=win_amd64
else
  os_name=manylinux_x86_64
fi

if [[ "$ARCHFLAGS" == *"arm64"* ]]
then
  export CMAKE_OSX_ARCHITECTURES="arm64;x86_64"
  platform="${os_name}_${mac_version}_universal2"
else
  export CMAKE_OSX_ARCHITECTURES="x86_64"
  platform="${os_name}_${mac_version}_x86_64"
fi

# Build libbson with the appropriate arch.
./build_libbson.sh

# Install pyarrow with the appropriate platform.
pip install --platform $platform --no-deps --only-binary=:all: pyarrow
