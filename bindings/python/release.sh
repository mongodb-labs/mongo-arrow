#!/bin/bash -ex

set -o xtrace
set -o errexit

# Set Python runtime to use via PYTHON_BINARY envvar
PYTHON=${PYTHON_BINARY:-"python"}

# Keep in sync with pymongoarrow.version._MIN_LIBBSON_VERSION
LIBBSON_REVISION="1.17.4"

# Compute shared library filename
if [ "Darwin" = "$(uname -s)" ]
then
  SO_EXT='dylib'
elif [ "Darwin" = "$(uname -s)" ]
then
  SO_EXT='so'
else
  echo "Unsupported platform"
fi
LIBBSON_SO="libbson-1.0.0.${SO_EXT}"

# Build libbson binaries in $(pwd)/libbson
./build-libbson.sh "$LIBBSON_REVISION"

# Export PKG_CONFIG_PATH to discover headers during build
if [ ! -d "$(pwd)/libbson/lib/pkgconfig" ]
then
  echo "libbson install did not publish pkg-config at expected location"
  exit 1
fi
export PKG_CONFIG_PATH="$(pwd)/libbson/lib/pkgconfig"

# Ensure we are in the correct working directory
if [ ! -d "$(pwd)/pymongoarrow" ] || [ ! -e "$(pwd)/setup.py" ]
then
  echo "PyMongoArrow sources not found at expected location"
  exit 1
fi

# Vendor libbson shared library in PyMongoArrow wheels
cp "$(pwd)/libbson/lib/${LIBBSON_SO}" "$(pwd)/pymongoarrow/"

# Install build dependencies
$PYTHON -m pip install -U pip setuptools
$PYTHON -m pip install wheel Cython>=0.29 pyarrow

# https://arrow.apache.org/docs/python/extending.html#building-extensions-against-pypi-wheels
$PYTHON -c "import pyarrow; pyarrow.create_library_symlinks()"

# Build wheels in $(pwd)/dist/*.whl
python setup.py clean --all
CFLAGS=$(pkg-config --cflags libbson-1.0) \
  LDFLAGS=$(pkg-config --libs libbson-1.0) \
  python setup.py bdist_wheel