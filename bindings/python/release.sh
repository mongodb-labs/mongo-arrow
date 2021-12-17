#!/bin/bash -ex

set -o xtrace
set -o errexit

# Ensure we are in the correct working directory
if [ ! -d "$(pwd)/pymongoarrow" ] || [ ! -e "$(pwd)/setup.py" ]
then
  echo "PyMongoArrow sources not found at expected location"
  exit 1
fi

# Build libbson binaries in $(pwd)/libbson
MONGO_LIBBSON_DIR="$(pwd)/libbson"

# Build libbson
MONGO_LIBBSON_DIR="$MONGO_LIBBSON_DIR" LIBBSON_VERSION=${LIBBSON_VERSION:-""} ./build-libbson.sh

# Print Python version used
python --version

# Install build dependencies
python -m pip install -U pip build

# Build wheels in $(pwd)/dist/*.whl
MONGO_LIBBSON_DIR="$MONGO_LIBBSON_DIR" python -m build --wheel .

# Run auditwheel repair to set platform tags on Linux
if [ "Linux" = "$(uname -s)" ]
then
  python -m pip install auditwheel
  python addtags.py dist/*.whl "$PLAT" ./wheelhouse
fi
