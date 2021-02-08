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
from datetime import datetime, timedelta
from unittest import TestCase

from pyarrow import Array, timestamp, int32, int64

from pymongoarrow.builder import (
    DatetimeBuilder, DoubleBuilder, Int32Builder, Int64Builder)


class TestIntBuildersMixin:
    def test_simple(self):
        builder = self.builder_cls()
        builder.append(0)
        builder.append_values([1, 2, 3, 4])
        builder.append(None)
        arr = builder.finish()

        self.assertIsInstance(arr, Array)
        self.assertEqual(arr.null_count, 1)
        self.assertEqual(len(arr), 6)
        self.assertEqual(
            arr.to_pylist(), [0, 1, 2, 3, 4, None])
        self.assertEqual(arr.type, self.data_type)


class TestInt32Builder(TestCase, TestIntBuildersMixin):
    def setUp(self):
        self.builder_cls = Int32Builder
        self.data_type = int32()


class TestInt64Builder(TestCase, TestIntBuildersMixin):
    def setUp(self):
        self.builder_cls = Int64Builder
        self.data_type = int64()


class TestDate64Builder(TestCase):
    def test_simple(self):
        # Check default unit
        builder = DatetimeBuilder()
        self.assertEqual(builder.unit, timestamp('ms'))

        # Milliseconds
        datetimes = [datetime(1970, 1, 1) + timedelta(milliseconds=k*100)
                     for k in range(5)]
        builder.append(datetimes[0])
        builder.append_values(datetimes[1:])
        builder.append(None)
        arr = builder.finish()

        self.assertIsInstance(arr, Array)
        self.assertEqual(arr.null_count, 1)
        self.assertEqual(len(arr), len(datetimes) + 1)
        self.assertEqual(arr.to_pylist(), datetimes + [None])
        self.assertEqual(arr.type, timestamp('ms'))

    def test_unsupported_units(self):
        with self.assertRaises(ValueError):
            DatetimeBuilder(dtype=timestamp('us'))

        with self.assertRaises(ValueError):
            DatetimeBuilder(dtype=timestamp('ns'))


class TestDoubleBuilder(TestCase):
    def test_simple(self):
        builder = DoubleBuilder()
        builder.append(0.123)
        builder.append_values([1.234, 2.345, 3.456, 4.567])
        builder.append(None)
        arr = builder.finish()

        self.assertIsInstance(arr, Array)
        self.assertEqual(arr.null_count, 1)
        self.assertEqual(len(arr), 6)
        self.assertEqual(
            arr.to_pylist(), [0.123, 1.234, 2.345, 3.456, 4.567, None])
