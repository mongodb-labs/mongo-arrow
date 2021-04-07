#!/bin/bash -ex

set -o xtrace
set -o errexit

LIBBSON_REVISION=$1
if [ -z "$LIBBSON_REVISION" ]
then
  echo "Did not provide a libbson revision ID to build"
  exit 1
fi

WORKDIR="mongo-c-driver-${LIBBSON_REVISION}"
if [ ! -d "$WORKDIR" ]
then
  git clone --depth 1 -b "$LIBBSON_REVISION" git@github.com:mongodb/mongo-c-driver.git "$WORKDIR"
fi

INSTALL_DIR="$(pwd)/libbson"

pushd "$WORKDIR"
  git checkout "$LIBBSON_REVISION"
  rm -rf cmake-build && mkdir cmake-build
  pushd cmake-build
    cmake -DENABLE_AUTOMATIC_INIT_AND_CLEANUP=OFF \
          -DENABLE_MONGOC=OFF \
          -DCMAKE_INSTALL_PREFIX:PATH="$INSTALL_DIR" \
          ..
    cmake --build .
    cmake --build . --target install
  popd
popd
