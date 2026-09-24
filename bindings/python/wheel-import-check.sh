#!/usr/bin/env bash
set -euxo pipefail
PY=$(command -v python3)
rm -rf dist
uv build --wheel --python "$PY"
WHEEL=$PWD/$(ls dist/pymongoarrow-*.whl)
# The oldest supported pyarrow is the exact build-time pin.
FLOOR=$(grep -oE 'pyarrow==[0-9.]+' pyproject.toml | head -1 | cut -d= -f3)
cd /tmp
for SPEC in "pyarrow==$FLOOR" ""; do
  VENV=/tmp/pymongoarrow-abi-check
  uv venv --clear -q --python "$PY" "$VENV"
  uv pip install -q --python "$VENV/bin/python" "$WHEEL" $SPEC
  "$VENV/bin/python" -c "from pymongoarrow.lib import libbson_version; import pyarrow; print('import OK with pyarrow', pyarrow.__version__)"
done
