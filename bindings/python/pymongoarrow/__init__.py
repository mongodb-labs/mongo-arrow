# Copyright 2021-present MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import os
# We must import pyarrow before attempting to load the Cython module.
import pyarrow

from pymongoarrow.version import __version__, _MIN_LIBBSON_VERSION
# This line must come second so setuptools can parse the __version__
# above without having a built application.
from pymongoarrow.lib import libbson_version

try:
    from pkg_resources import parse_version as _parse_version
except ImportError:
    from distutils.version import LooseVersion as _LooseVersion

    def _parse_version(version):
        return _LooseVersion(version)

# TODO: PYTHON-2659
on_rtd = os.environ.get('READTHEDOCS') == 'True'
if not on_rtd:
    if _parse_version(libbson_version) < _parse_version(_MIN_LIBBSON_VERSION):
        raise ImportError(
            f"Expected libbson version {_MIN_LIBBSON_VERSION} or greater, "
            f"found {libbson_version}")
