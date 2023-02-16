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
import numpy as np
import pytest
from bson import Binary
from pandas.tests.extension import base
from pymongoarrow.pandas_types import PandasBinary, PandasBinaryArray

try:
    base.BaseIndexTests
except AttributeError:
    pytest.skip("Not available", allow_module_level=True)


def make_datum():
    value = np.random.rand()
    return Binary(str(value).encode("utf8"), 10)


@pytest.fixture
def dtype():
    return PandasBinary(10)


def make_data():
    return (
        [make_datum() for _ in range(8)]
        + [np.nan]
        + [make_datum() for _ in range(88)]
        + [np.nan]
        + [make_datum(), make_datum()]
    )


@pytest.fixture
def data(dtype):
    return PandasBinaryArray(np.array(make_data(), dtype=object), dtype=dtype)


@pytest.fixture
def data_missing(dtype):
    return PandasBinaryArray(np.array([np.nan, make_datum()], dtype=object), dtype=dtype)


@pytest.fixture
def data_for_sorting(dtype):
    return PandasBinaryArray(
        np.array([make_datum(), make_datum(), make_datum()], dtype=object), dtype=dtype
    )


@pytest.fixture
def data_missing_for_sorting(dtype):
    return PandasBinaryArray(
        np.array([make_datum(), np.nan, make_datum()], dtype=object), dtype=dtype
    )


class TestDtype(base.BaseDtypeTests):
    def test_is_not_string_type(self, data):
        # Override to not return a value, which raises a warning.
        super().test_is_not_string_type(data)

    def test_is_not_object_type(self, data):
        # Override to not return a value, which raises a warning.
        super().test_is_not_object_type(data)


class TestInterface(base.BaseInterfaceTests):
    def test_array_interface(self):
        # Not implemented.
        pass

    def test_contains(self):
        # We cannot compare a Binary object to an array.
        pass


class TestConstructors(base.BaseConstructorsTests):
    def test_array_from_scalars(self):
        # Not applicable, must use dtype.
        pass


class TestGetitem(base.BaseGetitemTests):
    pass


class TestSetitem(base.BaseSetitemTests):
    def test_setitem_mask_boolean_array_with_na(self):
        # We cannot compare a Binary object to an array.
        pass

    def test_setitem_sequence_mismatched_length_raises(self):
        # Dtype used for array must be non-None.
        pass

    def test_setitem_frame_2d_values(self):
        # Results in passing an integer as a value, which
        # cannot be converted to Binary type.
        pass


class TestIndex(base.BaseIndexTests):
    pass


class TestMissing(base.BaseMissingTests):
    pass
