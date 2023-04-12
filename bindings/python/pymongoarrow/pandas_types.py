# Copyright 2023-present MongoDB, Inc.
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

# Pandas Extension Types

import numbers
import re
from typing import Type, Union

import numpy as np
import pandas as pd
import pyarrow as pa
from bson import Binary, Code, Decimal128, ObjectId
from pandas.api.extensions import (
    ExtensionArray,
    ExtensionDtype,
    register_extension_dtype,
)


class PandasBSONDtype(ExtensionDtype):
    """The base class for BSON Pandas extension data types."""

    na_value = np.nan

    @property
    def name(self) -> str:
        return f"bson_{self.__class__.__name__}"

    def __from_arrow__(self, array: Union[pa.Array, pa.ChunkedArray]) -> ExtensionArray:

        if isinstance(array, pa.Array):
            chunks = [array]
        else:
            # pyarrow.ChunkedArray
            chunks = array.chunks

        arr_type = self.construct_array_type()
        dtype = array.type.to_pandas_dtype()

        results = []
        for arr in chunks:
            # Convert low level values to the desired type.
            vals = []
            typ = self.type
            for val in np.array(arr):
                if not pd.isna(val) and not isinstance(val, typ):
                    if typ == Decimal128:
                        val = Decimal128.from_bid(val)
                    else:
                        val = typ(val)
                vals.append(val)
            arr = np.array(vals, dtype=object)
            # using _from_sequence to ensure None is converted to NA
            to_append = arr_type._from_sequence(arr, dtype=dtype)
            results.append(to_append)

        if results:
            return arr_type._concat_same_type(results)
        else:
            return arr_type(np.array([], dtype="object"))

    @classmethod
    def construct_from_string(cls, string):
        if not isinstance(string, str):
            raise TypeError(f"'construct_from_string' expects a string, got {type(string)}")
        default = cls()
        if string != default.name:
            raise TypeError(f"Cannot construct a '{cls.__name__}' from '{string}'")
        return default


class PandasBSONExtensionArray(ExtensionArray):
    """The base class for Pandas BSON extension arrays."""

    _default_dtype = None

    def __init__(self, values, dtype, copy=False) -> None:
        if not isinstance(values, np.ndarray):
            raise TypeError("Need to pass a numpy array as values")
        dtype = dtype or self._default_dtype
        if dtype is None:
            raise ValueError("dtype must be a valid data type")
        for val in values:
            if not isinstance(val, dtype.type) and not pd.isna(val):
                raise ValueError(f"Values must be either {dtype.type} or NA")
        self._dtype = dtype
        self.data = values

    @property
    def dtype(self):
        return self._dtype

    @classmethod
    def _from_sequence(cls, scalars, dtype=None, copy=False):
        data = np.empty(len(scalars), dtype=object)
        data[:] = scalars
        return cls(data, dtype=dtype)

    @classmethod
    def _from_factorized(cls, values, original):
        return cls(values, dtype=original.dtype)

    def __getitem__(self, item):
        if isinstance(item, numbers.Integral):
            return self.data[item]
        else:
            # slice, list-like, mask
            item = pd.api.indexers.check_array_indexer(self, item)
            return type(self)(self.data[item], dtype=self._dtype)

    def __setitem__(self, item, value):
        if (
            not hasattr(value, "__iter__")
            and not isinstance(value, self.dtype.type)
            and not pd.isna(value)
        ):
            raise ValueError(f"Value must be of type {self.dtype.type} or nan")
        if not isinstance(item, numbers.Integral):
            # slice, list-like, mask
            item = pd.api.indexers.check_array_indexer(self, item)
        elif not isinstance(value, self.dtype.type) and not pd.isna(value):
            raise ValueError(f"Array element must be of type {self.dtype.type} or nan")
        self.data[item] = value

    def __len__(self) -> int:
        return len(self.data)

    def isna(self):
        return np.array(
            [
                x is None or (x is not None and not isinstance(x, self.dtype.type) and np.isnan(x))
                for x in self.data
            ],
            dtype=bool,
        )

    def __eq__(self, other):
        return self.data == other

    def nbytes(self):
        return self.data.nbytes

    def take(self, indexer, allow_fill=False, fill_value=None):
        # re-implement here, since NumPy has trouble setting
        # sized objects like UserDicts into scalar slots of
        # an ndarary.
        indexer = np.asarray(indexer)
        msg = "Index is out of bounds or cannot do a "
        msg += "non-empty take from an empty array."

        if allow_fill:
            if fill_value is None:
                fill_value = self.dtype.na_value
            # bounds check
            if (indexer < -1).any():
                raise ValueError
            try:
                output = [self.data[loc] if loc != -1 else fill_value for loc in indexer]
            except IndexError as err:
                raise IndexError(msg) from err
        else:
            try:
                output = [self.data[loc] for loc in indexer]
            except IndexError as err:
                raise IndexError(msg) from err

        return self._from_sequence(output, self.dtype)

    def copy(self):
        return type(self)(self.data.copy(), dtype=self._dtype)

    @classmethod
    def _concat_same_type(cls, to_concat):
        data = np.concatenate([x.data for x in to_concat])
        dtype = to_concat[0].dtype
        return cls(data, dtype=dtype)


@register_extension_dtype
class PandasBinary(PandasBSONDtype):
    """A pandas extension type for BSON Binary data type."""

    type = Binary

    def __init__(self, subtype):
        self._subtype = subtype

    @property
    def subtype(self) -> int:
        return self._subtype

    @property
    def name(self) -> str:
        return f"bson_{self.type.__name__}[{self.subtype}]"

    @classmethod
    def construct_array_type(cls) -> Type["PandasBinaryArray"]:
        return PandasBinaryArray

    @classmethod
    def construct_from_string(cls, string):
        if not isinstance(string, str):
            raise TypeError(f"'construct_from_string' expects a string, got {type(string)}")
        pattern = re.compile(r"^bson_Binary\[(?P<subtype>.+)\]$")
        match = pattern.match(string)
        if match:
            return cls(**match.groupdict())
        else:
            raise TypeError(f"Cannot construct a '{cls.__name__}' from '{string}'")


class PandasBinaryArray(PandasBSONExtensionArray):
    """A pandas extension type for BSON Binary data arrays."""

    def __arrow_array__(self, type=None):
        from pymongoarrow.types import BinaryType

        return pa.array(self.data, type=BinaryType(self.dtype.subtype))

    def __eq__(self, other):
        # Binary types do not support element-wise comparison.
        if isinstance(other, Binary):
            other = np.array(other, dtype=object)
        return super().__eq__(other)


@register_extension_dtype
class PandasObjectId(PandasBSONDtype):
    """A pandas extension type for BSON ObjectId data type."""

    type = ObjectId

    @classmethod
    def construct_array_type(cls) -> Type["PandasObjectIdArray"]:
        return PandasObjectIdArray


class PandasObjectIdArray(PandasBSONExtensionArray):
    """A pandas extension type for BSON Binary data arrays."""

    @property
    def _default_dtype(self):
        return PandasObjectId()

    def __arrow_array__(self, type=None):
        from pymongoarrow.types import ObjectIdType

        return pa.array(self.data, type=ObjectIdType())


@register_extension_dtype
class PandasDecimal128(PandasBSONDtype):
    """A pandas extension type for BSON Decimal128 data type."""

    type = Decimal128

    @classmethod
    def construct_array_type(cls) -> Type["PandasDecimal128Array"]:
        return PandasDecimal128Array


class PandasDecimal128Array(PandasBSONExtensionArray):
    """A pandas extension type for BSON Binary data arrays."""

    @property
    def _default_dtype(self):
        return PandasDecimal128()

    def __arrow_array__(self, type=None):
        from pymongoarrow.types import Decimal128Type

        return pa.array(self.data, type=Decimal128Type())


@register_extension_dtype
class PandasCode(PandasBSONDtype):
    """A pandas extension type for BSON Code data type."""

    type = Code

    @classmethod
    def construct_array_type(cls) -> Type["PandasCodeArray"]:
        return PandasCodeArray


class PandasCodeArray(PandasBSONExtensionArray):
    """A pandas extension type for BSON Code data arrays."""

    @property
    def _default_dtype(self):
        return PandasCode()

    def __eq__(self, other):
        # Code types do not support element-wise comparison.
        if isinstance(other, Code):
            other = np.array(other, dtype=object)
        return super().__eq__(other)

    def __arrow_array__(self, type=None):
        from pymongoarrow.types import CodeType

        return pa.array(self.data, type=CodeType())
