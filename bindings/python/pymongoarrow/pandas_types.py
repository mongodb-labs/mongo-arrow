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
import numbers
from typing import Type, Union

import numpy as np
import pandas as pd
import pyarrow as pa
from bson import Binary
from pandas.api.extensions import (
    ExtensionArray,
    ExtensionDtype,
    register_extension_dtype,
)


class PandasBSONDtype(ExtensionDtype):
    na_value = np.nan

    @classmethod
    @property
    def name(cls) -> str:
        return f"bson_{cls.type}"

    def __from_arrow__(self, array: Union[pa.Array, pa.ChunkedArray]) -> ExtensionArray:

        if isinstance(array, pa.Array):
            chunks = [array]
        else:
            # pyarrow.ChunkedArray
            chunks = array.chunks

        arr_type = self.construct_array_type()

        results = []
        for arr in chunks:
            # Convert low level values to the desired type.
            vals = []
            typ = self.type
            for val in np.array(arr):
                if not pd.isna(val) and not isinstance(val, typ):
                    val = typ(val)
                vals.append(val)
            arr = np.array(vals)
            # using _from_sequence to ensure None is converted to NA
            to_append = arr_type._from_sequence(arr)
            results.append(to_append)

        if results:
            return arr_type._concat_same_type(results)
        else:
            return arr_type(np.array([], dtype="object"))


class PandasBSONExtensionArray(ExtensionArray):
    def __init__(self, values, dtype=None, copy=False) -> None:
        if not isinstance(values, np.ndarray):
            raise TypeError("Need to pass a numpy array as values")
        for val in values:
            if not isinstance(val, self.dtype.type) and not pd.isna(val):
                raise ValueError(f"Values must be either {self.dtype.type} or NA")
        self.data = values

    @classmethod
    def _from_sequence(cls, scalars, dtype=None, copy=False):
        data = np.empty(len(scalars), dtype=object)
        data[:] = scalars
        return cls(data)

    @classmethod
    def _from_factorized(cls, values, original):
        return cls(values, dtype=original.dtype)

    def __getitem__(self, item):
        if isinstance(item, numbers.Integral):
            return self.data[item]
        else:
            # slice, list-like, mask
            item = pd.api.indexers.check_array_indexer(self, item)
            return type(self)(self.data[item])

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
                x is not None and not isinstance(x, self.dtype.type) and np.isnan(x)
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

        return self._from_sequence(output)

    def copy(self):
        return type(self)(self.data.copy())

    @classmethod
    def _concat_same_type(cls, to_concat):
        data = np.concatenate([x.data for x in to_concat])
        return cls(data)


@register_extension_dtype
class PandasBSONBinary(PandasBSONDtype):
    type = Binary

    @classmethod
    def construct_array_type(cls) -> Type["PandasBSONArray"]:
        return PandasBSONArray


class PandasBSONArray(PandasBSONExtensionArray):
    dtype = PandasBSONBinary()
