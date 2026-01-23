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
from test.pandas_types.util import base_make_data

import numpy as np
import pytest
from bson import Binary

from pymongoarrow.pandas_types import PandasBinary, PandasBinaryArray

try:
    from pandas.tests.extension import base
except ImportError:
    pytest.skip("skipping pandas tests", allow_module_level=True)

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
    return base_make_data(make_datum)


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

    def test_array_interface_copy(self):
        # We cannot avoid copying with our extension arrays.
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
