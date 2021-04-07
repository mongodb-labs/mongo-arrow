#!/bin/bash -ex

set -o xtrace
set -o errexit

# Revision ID to build
LIBBSON_REVISION=$1
if [ -z "$LIBBSON_REVISION" ]
then
  echo "Did not provide a libbson revision ID to build"
  exit 1
fi

# Setup working directory
WORKDIR="mongo-c-driver-${LIBBSON_REVISION}"
if [ ! -d "$WORKDIR" ]
then
  git clone --depth 1 -b "$LIBBSON_REVISION" git@github.com:mongodb/mongo-c-driver.git "$WORKDIR"
fi

# Directory where build artifacts will be placed
INSTALL_DIR="$(pwd)/libbson"

# Build libbson
pushd "$WORKDIR"
  git checkout "$LIBBSON_REVISION"
  mkdir -p cmake-build
  pushd cmake-build
    cmake -DENABLE_AUTOMATIC_INIT_AND_CLEANUP=OFF \
          -DENABLE_MONGOC=OFF \
          -DCMAKE_INSTALL_PREFIX:PATH="$INSTALL_DIR" \
          ..
    cmake --build . --target clean
    cmake --build .
    cmake --build . --target install
  popd
popd
