#!/bin/bash -ex
cd /src
LIBBSON_INSTALL_DIR=$(pwd)/libbson
export LIBBSON_INSTALL_DIR=$LIBBSON_INSTALL_DIR
rm -rf $LIBBSON_INSTALL_DIR
./build-libbson.sh
rm -rf build dist
PYTHON=/opt/python/cp311-cp311/bin/python
$PYTHON -m pip install build
$PYTHON -m build --wheel .
rm -rf $LIBBSON_INSTALL_DIR
rm -rf build
