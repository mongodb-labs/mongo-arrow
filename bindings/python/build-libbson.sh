#!/bin/bash -ex

set -o xtrace
set -o errexit

# Version of libbson to build
# Keep in sync with pymongoarrow.version._MIN_LIBBSON_VERSION
LIBBSON_VERSION=${LIBBSON_VERSION:-"1.17.5"}
if [ -z "$LIBBSON_VERSION" ]
then
  echo "Did not provide a libbson revision ID to build"
  exit 1
fi

# Setup working directory
WORKDIR="mongo-c-driver-${LIBBSON_VERSION}"
if [ ! -d "$WORKDIR" ]
then
  git clone --depth 1 -b "$LIBBSON_VERSION" https://github.com/mongodb/mongo-c-driver.git "$WORKDIR"
fi

# Directory where build artifacts will be placed
LIBBSON_INSTALL_DIR=${LIBBSON_INSTALL_DIR:-""}

# Build libbson
pushd "$WORKDIR"
  git checkout "$LIBBSON_VERSION"
  mkdir -p cmake-build
  pushd cmake-build
    if [ -n "$LIBBSON_INSTALL_DIR" ]
    then
      echo "Installing libbson in $LIBBSON_INSTALL_DIR"
      cmake -DENABLE_AUTOMATIC_INIT_AND_CLEANUP=OFF \
            -DENABLE_MONGOC=OFF \
            -DCMAKE_OSX_DEPLOYMENT_TARGET="10.9" \
            -DCMAKE_INSTALL_PREFIX:PATH="$LIBBSON_INSTALL_DIR" \
            ..
    else
      cmake -DENABLE_AUTOMATIC_INIT_AND_CLEANUP=OFF \
            -DENABLE_MONGOC=OFF \
            -DCMAKE_OSX_DEPLOYMENT_TARGET="10.9" \
            ..
    fi
    cmake --build . --target clean
    cmake --build .
    cmake --build . --target install
  popd
popd
