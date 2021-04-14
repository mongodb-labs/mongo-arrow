#!/bin/bash -ex

set -o xtrace
set -o errexit

# Build libbson binaries in $(pwd)/libbson
LIBBSON_INSTALL_DIR="$(pwd)/libbson"
LIBBSON_INSTALL_DIR="$LIBBSON_INSTALL_DIR" LIBBSON_VERSION=${LIBBSON_VERSION:-""} ./build-libbson.sh

# Ensure we are in the correct working directory
if [ ! -d "$(pwd)/pymongoarrow" ] || [ ! -e "$(pwd)/setup.py" ]
then
  echo "PyMongoArrow sources not found at expected location"
  exit 1
fi

# Platform-dependent actions:
# - Compute shared library path
# - Set Python runtime to use
if [ "Darwin" = "$(uname -s)" ]
then
  LIBBSON_PATH="$LIBBSON_INSTALL_DIR/lib/libbson-1.0.0.dylib"
  PYTHON=${PYTHON_BINARY:-"python"}
  export MACOSX_DEPLOYMENT_TARGET="10.9"
elif [ "Linux" = "$(uname -s)" ]
then
  LIBBSON_PATH="$LIBBSON_INSTALL_DIR/lib64/libbson-1.0.so.0"
  PYTHON=${PYTHON_BINARY:-"/opt/python/cp37-cp37m/bin/python"}
else
  echo "Unsupported platform"
fi

# Print Python version used
$PYTHON --version

# Vendor libbson shared library in PyMongoArrow wheels
cp "$LIBBSON_PATH" "$(pwd)/pymongoarrow/"

# Ensure a clean build
rm -rf build "$(pwd)/pymongoarrow/*.so" "$(pwd)/pymongoarrow/*.dylib"

# Install build dependencies
$PYTHON -m pip install -U pip setuptools wheel
$PYTHON -m pip install "Cython>=0.29" "pyarrow>=3,<3.1"

# https://arrow.apache.org/docs/python/extending.html#building-extensions-against-pypi-wheels
$PYTHON -c "import pyarrow; pyarrow.create_library_symlinks()"

# Build wheels in $(pwd)/dist/*.whl
LIBBSON_INSTALL_DIR="$LIBBSON_INSTALL_DIR" $PYTHON setup.py bdist_wheel
