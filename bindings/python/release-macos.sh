#!/bin/bash -ex

set -o xtrace
set -o errexit

PYTHON="python"
LIBBSON_REVISION="1.17.4"
LIBBSON_SO="libbson-1.0.0.dylib"

#./build-libbson.sh "$LIBBSON_REVISION"

if [ ! -d "$(pwd)/libbson/lib/pkgconfig" ]
then
  echo "libbson install did not publish pkg-config at expected location"
  exit 1
fi

if [ ! -d "$(pwd)/pymongoarrow" ]
then
  echo "PyMongoArrow source directory not found at expected location"
  exit 1
fi

if [ ! -f "$(pwd)/setup.py" ]
then
  echo "PyMongoArrow setup.py not found at expected location"
  exit 1
fi

# Vendor libbson shared library in PyMongoArrow wheels
cp "$(pwd)/libbson/lib/${LIBBSON_SO}" "$(pwd)/pymongoarrow/"

# Install build dependencies
$PYTHON -m pip install -U pip setuptools
$PYTHON -m pip install wheel Cython>=0.29 pyarrow
$PYTHON -c "import pyarrow; pyarrow.create_library_symlinks()"

# Use pkg-config to discover headers during build
export PKG_CONFIG_PATH="$(pwd)/libbson/lib/pkgconfig"
CFLAGS=$(pkg-config --cflags libbson-1.0) LDFLAGS=$(pkg-config --libs libbson-1.0) python setup.py bdist_wheel
