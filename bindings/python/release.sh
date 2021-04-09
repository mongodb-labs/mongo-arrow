#!/bin/bash -ex

set -o xtrace
set -o errexit

# Set Python runtime to use via PYTHON_BINARY envvar
PYTHON=${PYTHON_BINARY:-"python"}
$PYTHON --version

# Compute shared library filename
if [ "Darwin" = "$(uname -s)" ]
then
  SO_EXT='dylib'
elif [ "Darwin" = "$(uname -s)" ]
then
  SO_EXT='so'
  export MACOSX_DEPLOYMENT_TARGET="10.9"
else
  echo "Unsupported platform"
fi

# Build libbson binaries in $(pwd)/libbson
LIBBSON_INSTALL_DIR="$(pwd)/libbson"
LIBBSON_INSTALL_DIR="$LIBBSON_INSTALL_DIR" LIBBSON_VERSION=${LIBBSON_VERSION:-""} ./build-libbson.sh

# Ensure we are in the correct working directory
if [ ! -d "$(pwd)/pymongoarrow" ] || [ ! -e "$(pwd)/setup.py" ]
then
  echo "PyMongoArrow sources not found at expected location"
  exit 1
fi

# Vendor libbson shared library in PyMongoArrow wheels
cp "$(pwd)/libbson/lib/libbson-1.0.0.${SO_EXT}" "$(pwd)/pymongoarrow/"

# Install build dependencies
$PYTHON -m pip install -U pip setuptools wheel
$PYTHON -m pip install "Cython>=0.29" "pyarrow>=3,<3.1"

# https://arrow.apache.org/docs/python/extending.html#building-extensions-against-pypi-wheels
$PYTHON -c "import pyarrow; pyarrow.create_library_symlinks()"

# Build wheels in $(pwd)/dist/*.whl
python setup.py clean --all
LIBBSON_INSTALL_DIR="$LIBBSON_INSTALL_DIR" python setup.py bdist_wheel
