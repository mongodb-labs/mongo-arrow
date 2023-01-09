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
import enum
from datetime import datetime

import numpy as np
import pyarrow as pa
import pyarrow.types as _atypes
from bson import Decimal128, Int64, ObjectId
from pyarrow import DataType as _ArrowDataType
from pyarrow import (
    ExtensionScalar,
    PyExtensionType,
    binary,
    bool_,
    float64,
    int64,
    list_,
    string,
    struct,
    timestamp,
)


class _BsonArrowTypes(enum.Enum):
    datetime = 1
    double = 2
    int32 = 3
    int64 = 4
    objectid = 5
    string = 6
    bool = 7
    decimal128_str = 8
    document = 9
    array = 10


# Custom Extension Types.
# See https://arrow.apache.org/docs/python/extending_types.html#defining-extension-types-user-defined-types
# for details.


class ObjectIdScalar(ExtensionScalar):
    def as_py(self):
        return ObjectId(self.value.as_py())


class ObjectIdType(PyExtensionType):
    _type_marker = _BsonArrowTypes.objectid

    def __init__(self):
        super().__init__(binary(12))

    def __reduce__(self):
        return ObjectIdType, ()

    def __arrow_ext_scalar_class__(self):
        return ObjectIdScalar


class Decimal128Scalar(ExtensionScalar):
    def as_py(self):
        return Decimal128(self.value.as_py())


class Decimal128StringType(PyExtensionType):
    _type_marker = _BsonArrowTypes.decimal128_str

    def __init__(self):
        super().__init__(string())

    def __reduce__(self):
        return Decimal128StringType, ()

    def __arrow_ext_scalar_class__(self):
        return Decimal128Scalar


# Internal Type Handling.


def _is_objectid(obj):
    type_marker = getattr(obj, "_type_marker", "")
    return type_marker == ObjectIdType._type_marker


def _is_decimal128_str(obj):
    type_marker = getattr(obj, "_type_marker", "")
    return type_marker == Decimal128StringType._type_marker


_TYPE_NORMALIZER_FACTORY = {
    Int64: lambda _: int64(),
    float: lambda _: float64(),
    int: lambda _: int64(),
    # Note: we cannot infer a timezone form a raw datetime class,
    # if a timezone is preferred then a timestamp with tz information
    # must be used directly.
    datetime: lambda _: timestamp("ms"),
    ObjectId: lambda _: ObjectIdType(),
    Decimal128: lambda _: Decimal128StringType(),
    str: lambda _: string(),
    bool: lambda _: bool_(),
}


_TYPE_CHECKER_TO_NUMPY = {
    _atypes.is_int32: np.int32,
    _atypes.is_int64: np.int64,
    _atypes.is_float64: np.float64,
    _atypes.is_timestamp: "datetime64[ms]",
    _is_objectid: object,
    _atypes.is_string: np.str_,
    _atypes.is_boolean: np.bool_,
}


def get_numpy_type(type):
    for checker, comp_type in _TYPE_CHECKER_TO_NUMPY.items():
        if checker(type):
            return comp_type
    return None


_TYPE_CHECKER_TO_INTERNAL_TYPE = {
    _atypes.is_int32: _BsonArrowTypes.int32,
    _atypes.is_int64: _BsonArrowTypes.int64,
    _atypes.is_float64: _BsonArrowTypes.double,
    _atypes.is_timestamp: _BsonArrowTypes.datetime,
    _is_objectid: _BsonArrowTypes.objectid,
    _is_decimal128_str: _BsonArrowTypes.decimal128_str,
    _atypes.is_string: _BsonArrowTypes.string,
    _atypes.is_boolean: _BsonArrowTypes.bool,
    _atypes.is_struct: _BsonArrowTypes.document,
    _atypes.is_list: _BsonArrowTypes.array,
}


def _is_typeid_supported(typeid):
    return typeid in _TYPE_NORMALIZER_FACTORY


def _normalize_typeid(typeid, field_name):
    if isinstance(typeid, _ArrowDataType):
        return typeid
    elif isinstance(typeid, dict):
        fields = []
        for sub_field_name, sub_typeid in typeid.items():
            fields.append((sub_field_name, _normalize_typeid(sub_typeid, sub_field_name)))
        return struct(fields)
    elif isinstance(typeid, list):
        return list_(_normalize_typeid(type(typeid[0]), "0"))
    elif _is_typeid_supported(typeid):
        normalizer = _TYPE_NORMALIZER_FACTORY[typeid]
        return normalizer(typeid)
    else:
        raise ValueError("Unsupported type identifier {} for field {}".format(typeid, field_name))


def _get_internal_typemap(typemap):
    internal_typemap = {}
    for fname, ftype in typemap.items():
        for checker, internal_id in _TYPE_CHECKER_TO_INTERNAL_TYPE.items():
            if checker(ftype):
                internal_typemap[fname] = internal_id

        if fname not in internal_typemap:
            raise ValueError(
                f'Unsupported data type in schema for field "{fname}" of type "{ftype}"'
            )

    return internal_typemap


def _in_type_map(t):
    if isinstance(t, np.dtype):
        try:
            t = pa.from_numpy_dtype(t)
        except pa.lib.ArrowNotImplementedError:
            return False
    for checker in _TYPE_CHECKER_TO_INTERNAL_TYPE.keys():
        if checker(t):
            return True
    return False


def _validate_schema(schema):
    for i in schema:
        if not _in_type_map(i):
            raise ValueError(f'Unsupported data type "{i}" in schema')
