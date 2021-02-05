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
from unittest import TestCase

from pyarrow import Array, int32, int64

from pymongoarrow.builder import Int32Builder, Int64Builder


class TestIntBuildersMixin:
    def test_simple(self):
        builder = self.builder_cls()
        builder.append(0)
        builder.append_values([1, 2, 3, 4])
        arr = builder.finish()

        self.assertIsInstance(arr, Array)
        self.assertEqual(arr.null_count, 0)
        self.assertEqual(len(arr), 5)
        self.assertEqual(
            arr.to_pylist(), [0, 1, 2, 3, 4])
        self.assertEqual(arr.type, self.data_type)


class TestInt32Builder(TestCase, TestIntBuildersMixin):
    def setUp(self):
        self.builder_cls = Int32Builder
        self.data_type = int32()


class TestInt64Builder(TestCase, TestIntBuildersMixin):
    def setUp(self):
        self.builder_cls = Int64Builder
        self.data_type = int64()
