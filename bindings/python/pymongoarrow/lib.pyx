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

# Cython compiler directives
# distutils: language=c++
# cython: language_level=3

# Stdlib imports
import copy
import datetime
import enum

# Python imports
import numpy as np
from pyarrow import timestamp
from pymongoarrow.types import _BsonArrowTypes
from pymongoarrow.errors import InvalidBSON, PyMongoArrowError

# Cython imports
from cpython cimport PyBytes_Size, object
from cython.operator cimport dereference
from libcpp cimport bool as cbool
from libcpp.map cimport map
from libcpp.string cimport string
from pyarrow.lib cimport *
from pymongoarrow.libbson cimport *
from pymongoarrow.types import _BsonArrowTypes


# libbson version
libbson_version = bson_get_version().decode('utf-8')

# BSON tools
include "bson.pyi"

# Builders
include "builders.pyi"
