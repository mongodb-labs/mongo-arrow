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

from pymongoarrow.libbson.version import __version__ as libbson_version
from pymongoarrow.version import __version__, _MIN_LIBBSON_VERSION


try:
    from pkg_resources import parse_version as _parse_version
except ImportError:
    from distutils.version import LooseVersion as _LooseVersion

    def _parse_version(version):
        return _LooseVersion(version)


if _parse_version(libbson_version) < _parse_version(_MIN_LIBBSON_VERSION):
    raise ImportError(
        "Expected libbson version {} or greater, found {}}".format(
            _MIN_LIBBSON_VERSION, libbson_version))


from pymongoarrow.api import aggregate_arrow_all, find_arrow_all
from pymongoarrow.schema import Schema


__all__ = [
    'aggregate_arrow_all',
    'find_arrow_all',
    'Schema'
]
