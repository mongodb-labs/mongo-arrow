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
from bson import Binary, Code, Decimal128, Int64, ObjectId
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
from pymongoarrow.pandas_types import (
    PandasBinary,
    PandasCode,
    PandasDecimal128,
    PandasObjectId,
)


class _BsonArrowTypes(enum.Enum):
    datetime = 1
    double = 2
    int32 = 3
    int64 = 4
    objectid = 5
    string = 6
    bool = 7
    decimal128 = 8
    document = 9
    array = 10
    binary = 11
    code = 12


# Custom Extension Types.
# See https://arrow.apache.org/docs/python/extending_types.html#defining-extension-types-user-defined-types
# for details.


class BSONExtensionScalar(ExtensionScalar):
    def as_py(self):
        if self.value is None:
            return None
        return self._bson_class(self.value.as_py())


class ObjectIdScalar(BSONExtensionScalar):
    _bson_class = ObjectId


class ObjectIdType(PyExtensionType):
    _type_marker = _BsonArrowTypes.objectid

    def __init__(self):
        super().__init__(binary(12))

    def __reduce__(self):
        return ObjectIdType, ()

    def __arrow_ext_scalar_class__(self):
        return ObjectIdScalar

    def to_pandas_dtype(self):
        return PandasObjectId()


class Decimal128Scalar(ExtensionScalar):
    def as_py(self):
        if self.value is None:
            return None
        return Decimal128.from_bid(self.value.as_py())


class Decimal128Type(PyExtensionType):
    _type_marker = _BsonArrowTypes.decimal128

    def __init__(self):
        super().__init__(binary(16))

    def __reduce__(self):
        return Decimal128Type, ()

    def __arrow_ext_scalar_class__(self):
        return Decimal128Scalar

    def to_pandas_dtype(self):
        return PandasDecimal128()


class BinaryScalar(ExtensionScalar):
    def as_py(self):
        value = self.value
        if value is None:
            return None
        return Binary(self.value.as_py(), self.type.subtype)


class BinaryType(PyExtensionType):
    _type_marker = _BsonArrowTypes.binary

    def __init__(self, subtype):
        self._subtype = subtype
        super().__init__(binary())

    @property
    def subtype(self):
        return self._subtype

    def __reduce__(self):
        return BinaryType, (self._subtype,)

    def __arrow_ext_scalar_class__(self):
        return BinaryScalar

    def to_pandas_dtype(self):
        return PandasBinary(self.subtype)


class CodeScalar(BSONExtensionScalar):
    _bson_class = Code


class CodeType(PyExtensionType):
    _type_marker = _BsonArrowTypes.code

    def __init__(self):
        super().__init__(string())

    def __reduce__(self):
        return CodeType, ()

    def __arrow_ext_scalar_class__(self):
        return CodeScalar

    def to_pandas_dtype(self):
        return PandasCode()


# Internal Type Handling.


def _is_objectid(obj):
    type_marker = getattr(obj, "_type_marker", "")
    return type_marker == ObjectIdType._type_marker


def _is_decimal128(obj):
    type_marker = getattr(obj, "_type_marker", "")
    return type_marker == Decimal128Type._type_marker


def _is_binary(obj):
    type_marker = getattr(obj, "_type_marker", "")
    return type_marker == BinaryType._type_marker


def _is_code(obj):
    type_marker = getattr(obj, "_type_marker", "")
    return type_marker == CodeType._type_marker


_TYPE_NORMALIZER_FACTORY = {
    Int64: lambda _: int64(),
    float: lambda _: float64(),
    int: lambda _: int64(),
    # Note: we cannot infer a timezone form a raw datetime class,
    # if a timezone is preferred then a timestamp with tz information
    # must be used directly.
    datetime: lambda _: timestamp("ms"),
    ObjectId: lambda _: ObjectIdType(),
    Decimal128: lambda _: Decimal128Type(),
    str: lambda _: string(),
    bool: lambda _: bool_(),
    Binary: lambda subtype: BinaryType(subtype),
    Code: lambda _: CodeType(),
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
    _is_decimal128: _BsonArrowTypes.decimal128,
    _is_binary: _BsonArrowTypes.binary,
    _is_code: _BsonArrowTypes.code,
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
