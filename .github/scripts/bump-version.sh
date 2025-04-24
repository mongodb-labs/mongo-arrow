#!/usr/bin/env bash
set -eu

CURRENT_VERSION=$(NO_EXT=1 python setup.py --version)
echo "HELLO $CURRENT_VERSION"
sed -i '' "s/__version__ = \"${CURRENT_VERSION}\"/__version__ = \"$1\"/" "pymongoarrow/version.py"
