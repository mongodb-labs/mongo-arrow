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
    platform="macosx_${mac_version}_universal2"
    export CMAKE_OSX_ARCHITECTURES="arm64;x86_64"
  else
    platform="macosx_${mac_version}_x86_64"
  fi

  # Install pyarrow with the appropriate platform.
  pip install --platform $platform --target $HOME/wheels --no-deps --only-binary=:all: pyarrow
fi

pip install --user pyarrow
ARROW_LIB=$(python -c "import pyarrow;print(':'.join(pyarrow.get_library_dirs()))")
ls -ltr $ARROW_LIB
export LD_LIBRARY_PATH="$ARROW_LIB:$LD_LIBRARY_PATH"


# Build libbson with the appropriate arch.
source ./build-libbson.sh


echo "LD_LIBRARY_PATH: $LD_LIBRARY_PATH"
