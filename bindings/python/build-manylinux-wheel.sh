#!/bin/bash -ex

set -o xtrace
set -o errexit

image="quay.io/pypa/manylinux2014_x86_64"
docker pull $image
docker run --rm -v "`pwd`:/src" $image /src/build-manylinux-wheel-internal.sh
