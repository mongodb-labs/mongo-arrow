#!/bin/bash -ex

set -o xtrace
set -o errexit

pip install pyarrow
ARROW_LIB=$(python -c "import pyarrow;print(':'.join(pyarrow.get_library_dirs()))")
ls -ltr $ARROW_LIB
export REPAIR_LIBRARY_PATH="$ARROW_LIB:$LD_LIBRARY_PATH"
