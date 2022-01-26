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
from datetime import datetime
import enum

from bson import Int64, ObjectId

from pyarrow import timestamp, float64, int64, int32, string
from pyarrow import DataType as _ArrowDataType
import pyarrow.types as _atypes


class _BsonArrowTypes(enum.Enum):
    datetime = 1
    double = 2
    int32 = 3
    int64 = 4
    objectid = 5
    string = 6


_TYPE_NORMALIZER_FACTORY = {
    Int64: lambda _: int64(),
    float: lambda _: float64(),
    int: lambda _: int64(),
    datetime: lambda _: timestamp('ms'),     # TODO: add tzinfo support
    ObjectId: lambda _: ObjectId,
    str: lambda: string()
}


def _is_objectid(obj):
    return obj == ObjectId


_TYPE_CHECKER_TO_INTERNAL_TYPE = {
    _atypes.is_int32: _BsonArrowTypes.int32,
    _atypes.is_int64: _BsonArrowTypes.int64,
    _atypes.is_float64: _BsonArrowTypes.double,
    _atypes.is_timestamp: _BsonArrowTypes.datetime,
    _is_objectid: _BsonArrowTypes.objectid,
    _atypes.is_string: _BsonArrowTypes.string
}


def _is_typeid_supported(typeid):
    return typeid in _TYPE_NORMALIZER_FACTORY


def _normalize_typeid(typeid, field_name):
    if isinstance(typeid, _ArrowDataType):
        return typeid
    elif _is_typeid_supported(typeid):
        normalizer = _TYPE_NORMALIZER_FACTORY[typeid]
        return normalizer(typeid)
    else:
        raise ValueError(
            "Unsupported type identifier {} for field {}".format(
                typeid, field_name))


def _get_internal_typemap(typemap):
    internal_typemap = {}
    for fname, ftype in typemap.items():
        for checker, internal_id in _TYPE_CHECKER_TO_INTERNAL_TYPE.items():
            # Catch error where the pyarrow checkers are looking for an `id`
            # attribute that might not exist on non-pyarrow types
            # (like ObjectId).  For example, `is_int32()` checks for
            # `t.id == lib.Type_INT32`.
            try:
                if checker(ftype):
                    internal_typemap[fname] = internal_id
            except AttributeError:
                pass
        if fname not in internal_typemap:
            raise ValueError('Unsupported data type in schema for ' +
                f'field "{fname}" of type "{ftype}"')

    return internal_typemap
