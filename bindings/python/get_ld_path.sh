#!/bin/bash -ex

set -o xtrace
set -o errexit

pip install pyarrow
ARROW_LIB=$(python -c "import pyarrow;print(':'.join(pyarrow.get_library_dirs()))")
ls -ltr $ARROW_LIB
LIBBSON=$(pwd)/libbson/lib64
export REPAIR_LIBRARY_PATH="$ARROW_LIB:$LIBBSON:$LD_LIBRARY_PATH"
