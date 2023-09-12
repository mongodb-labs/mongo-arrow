#!/bin/bash -ex

set -o xtrace
set -o errexit

# Requirements:
# build-essential cmake python3-dev python3.10-venv libssl-dev
git clone https://github.com/apache/arrow.git
pushd arrow
git submodule update --init
popd
pip install -r arrow/python/requirements-build.txt
DIST=$(pwd)/arrow-dist
mkdir $DIST
mkdir arrow/cpp/build
pushd arrow/cpp/build
export ARROW_HOME=$DIST
export LD_LIBRARY_PATH="$DIST/lib:$LD_LIBRARY_PATH"
export CMAKE_PREFIX_PATH="$ARROW_HOME:$CMAKE_PREFIX_PATH"
cmake -DCMAKE_INSTALL_PREFIX=$ARROW_HOME \
        -DCMAKE_INSTALL_LIBDIR=lib \
        -DCMAKE_BUILD_TYPE=Debug \
        -DARROW_BUILD_TESTS=ON \
        -DARROW_COMPUTE=ON \
        -DARROW_CSV=ON \
        -DARROW_DATASET=ON \
        -DARROW_FILESYSTEM=ON \
        -DARROW_HDFS=ON \
        -DARROW_JSON=ON \
        -DARROW_PARQUET=ON \
        -DARROW_WITH_BROTLI=ON \
        -DARROW_WITH_BZ2=ON \
        -DARROW_WITH_LZ4=ON \
        -DARROW_WITH_SNAPPY=ON \
        -DARROW_WITH_ZLIB=ON \
        -DARROW_WITH_ZSTD=ON \
        -DPARQUET_REQUIRE_ENCRYPTION=ON \
        ..
make -j4
make install
popd

pushd arrow/python
export PYARROW_WITH_PARQUET=1
export PYARROW_WITH_DATASET=1
export PYARROW_PARALLEL=4
pip install wheel
pip install -e . --no-build-isolation
popd
