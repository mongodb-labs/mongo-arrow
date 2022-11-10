#!/bin/bash -ex

set -o xtrace
set -o errexit

# Ensure we are in the correct working directory
if [ ! -d "$(pwd)/pymongoarrow" ] || [ ! -e "$(pwd)/setup.py" ]
then
  echo "PyMongoArrow sources not found at expected location"
  exit 1
fi

# Clean up
rm -rf dist wheelhouse build pymongoarrow/*.so pymongoarrow/*.dll pymongoarrow/*.dylib

# Platform-dependent actions:
PYTHON=${PYTHON_BINARY:-"python"}
if [ "Linux" = "$(uname -s)" ]
then
  PYTHON=${PYTHON_BINARY:-"python3"}
fi

# Build libbson binaries in $(pwd)/libbson
LIBBSON_INSTALL_DIR="$(pwd)/libbson"

# Build libbson
LIBBSON_INSTALL_DIR="$LIBBSON_INSTALL_DIR" LIBBSON_VERSION=${LIBBSON_VERSION:-""} CMAKE_BUILD_TYPE=Release ./build-libbson.sh

# Print Python version used
$PYTHON --version

# Install build dependencies
$PYTHON -m pip install -U pip build

# Build wheels in $(pwd)/dist/*.whl
LIBBSON_INSTALL_DIR="$LIBBSON_INSTALL_DIR" $PYTHON -m build --wheel .

# Run auditwheel repair to set platform tags on Linux
if [ "Linux" = "$(uname -s)" ]
then
  $PYTHON -m pip install auditwheel
  $PYTHON addtags.py dist/*.whl "$PLAT" ./wheelhouse
fi

# Repair the wheel, copying shared libraries as needed
MACHINE=$(python -c "import platform;print(platform.machine())")
python repair_wheel.py "./wheelhouse" dist/*.whl $MACHINE
