#!/usr/bin/env bash
set -eu

CURRENT_VERSION=$(sed -n 's/^version = "\(.*\)"/\1/p' pyproject.toml)
sed -i "s/version = \"${CURRENT_VERSION}\"/version = \"$1\"/" pyproject.toml
