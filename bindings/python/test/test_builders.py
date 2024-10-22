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

from datetime import datetime, timedelta, timezone
from unittest import TestCase

from bson import Binary, Code, Decimal128, ObjectId, encode
from pyarrow import Array, bool_, int32, int64, timestamp

from pymongoarrow.lib import (
    BinaryBuilder,
    BoolBuilder,
    BuilderManager,
    CodeBuilder,
    Date32Builder,
    Date64Builder,
    DatetimeBuilder,
    Decimal128Builder,
    DocumentBuilder,
    DoubleBuilder,
    Int32Builder,
    Int64Builder,
    ListBuilder,
    ObjectIdBuilder,
    StringBuilder,
)


class IntBuildersTestMixin:
    def test_simple(self):
        builder = self.builder_cls()
        builder.append(0)
        builder.append_values([1, 2, 3, 4])
        builder.append("a")
        builder.append_null()
        arr = builder.finish()

        self.assertIsInstance(arr, Array)
        self.assertEqual(arr.null_count, 2)
        self.assertEqual(len(arr), 7)
        self.assertEqual(arr.to_pylist(), [0, 1, 2, 3, 4, None, None])
        self.assertEqual(arr.type, self.data_type)


class TestInt32Builder(TestCase, IntBuildersTestMixin):
    def setUp(self):
        self.builder_cls = Int32Builder
        self.data_type = int32()


class TestInt64Builder(TestCase, IntBuildersTestMixin):
    def setUp(self):
        self.builder_cls = Int64Builder
        self.data_type = int64()


class TestDatetimeBuilder(TestCase):
    def test_default_unit(self):
        # Check default unit
        builder = DatetimeBuilder()
        self.assertEqual(builder.unit, timestamp("ms"))

    def test_simple(self):
        self.maxDiff = None

        builder = DatetimeBuilder(dtype=timestamp("ms"))
        datetimes = [datetime.now(timezone.utc) + timedelta(days=k * 100) for k in range(5)]
        builder.append(datetimes[0])
        builder.append_values(datetimes[1:])
        builder.append(1)
        builder.append_null()
        arr = builder.finish()

        self.assertIsInstance(arr, Array)
        self.assertEqual(arr.null_count, 2)
        self.assertEqual(len(arr), len(datetimes) + 2)
        for actual, expected in zip(arr, datetimes + [None, None]):
            if actual.is_valid:
                self.assertEqual(actual.as_py().timetuple(), expected.timetuple())
            else:
                self.assertIsNone(expected)
        self.assertEqual(arr.type, timestamp("ms"))

    def test_unsupported_units(self):
        for unit in ("s", "us", "ns"):
            with self.assertRaises(TypeError):
                DatetimeBuilder(dtype=timestamp(unit))


class TestDoubleBuilder(TestCase):
    def test_simple(self):
        builder = DoubleBuilder()
        values = [0.123, 1.234, 2.345, 3.456, 4.567, 1]
        builder.append(values[0])
        builder.append_values(values[1:])
        builder.append("a")
        builder.append_null()
        arr = builder.finish()

        self.assertIsInstance(arr, Array)
        self.assertEqual(arr.null_count, 2)
        self.assertEqual(len(arr), 8)
        self.assertEqual(arr.to_pylist(), values + [None, None])


class TestObjectIdBuilder(TestCase):
    def test_simple(self):
        ids = [ObjectId() for i in range(5)]
        builder = ObjectIdBuilder()
        builder.append(ids[0])
        builder.append_values(ids[1:])
        builder.append(b"123456789123")
        builder.append_null()
        arr = builder.finish()

        self.assertIsInstance(arr, Array)
        self.assertEqual(arr.null_count, 2)
        self.assertEqual(len(arr), 7)
        self.assertEqual(arr.to_pylist(), ids + [None, None])


class TestStringBuilder(TestCase):
    def test_simple(self):
        # Greetings in various languages, from
        # https://www.w3.org/2001/06/utf-8-test/UTF-8-demo.html
        values = ["Hello world", "Καλημέρα κόσμε", "コンニチハ"]
        values += ["hello\u0000world"]
        builder = StringBuilder()
        builder.append(values[0])
        builder.append_values(values[1:])
        builder.append(b"1")
        builder.append_null()
        arr = builder.finish()

        self.assertIsInstance(arr, Array)
        self.assertEqual(arr.null_count, 2)
        self.assertEqual(len(arr), 6)
        self.assertEqual(arr.to_pylist(), values + [None, None])


class TestDocumentBuilder(TestCase):
    def test_simple(self):
        manager = BuilderManager({}, False, None)
        builder = DocumentBuilder(manager, b"foo")
        manager.builder_map[b"foo"] = builder
        builder.append(dict(a=1, b=2, c=3))
        builder.append_null()
        names = builder.finish()
        assert names == set((b"a", b"b", b"c"))
        child = manager.builder_map[b"foo.a"]
        assert child.finish().to_pylist() == [1, None]
        child = manager.builder_map[b"foo.b"]
        assert child.finish().to_pylist() == [2, None]
        child = manager.builder_map[b"foo.c"]
        assert child.finish().to_pylist() == [3, None]


class TestListBuilder(TestCase):
    def test_simple(self):
        manager = BuilderManager({}, False, None)
        builder = ListBuilder(manager, b"foo")
        manager.builder_map[b"foo"] = builder
        builder.append([1, 2])
        # TODO: test appending nulls in the middle.
        builder.append([3, 4, 5])
        # builder.append_null()
        manager.finish()
        arr = manager.builder_map[b"foo"]
        assert arr.to_pylist() == None


class TestBuilderManager(TestCase):
    def test_simple(self):
        manager = BuilderManager({}, False, None)
        data = b"".join(encode(d) for d in [dict(a=1), dict(a=2), dict(a=None)])
        manager.process_bson_stream(data, len(data))
        builder_map = manager.finish()
        assert list(builder_map) == [b"a"]
        assert list(builder_map.values())[0].to_pylist() == [1, 2, None]

    def test_nested(self):
        raise NotImplementedError()


class TestBinaryBuilder(TestCase):
    def test_simple(self):
        data = [Binary(bytes(i), 10) for i in range(5)]
        builder = BinaryBuilder(10)
        builder.append(data[0])
        builder.append_values(data[1:])
        builder.append(1)
        builder.append_null()
        arr = builder.finish()

        self.assertIsInstance(arr, Array)
        self.assertEqual(arr.null_count, 2)
        self.assertEqual(len(arr), 7)
        self.assertEqual(arr.to_pylist(), data + [None, None])


class TestDecimal128Builder(TestCase):
    def test_simple(self):
        data = [Decimal128([i, i]) for i in range(5)]
        builder = Decimal128Builder()
        builder.append(data[0])
        builder.append_values(data[1:])
        builder.append(1)
        builder.append_null()
        arr = builder.finish()

        self.assertIsInstance(arr, Array)
        self.assertEqual(arr.null_count, 2)
        self.assertEqual(len(arr), 7)
        self.assertEqual(arr.to_pylist(), data + [None, None])


class BoolBuilderTestMixin:
    def test_simple(self):
        builder = BoolBuilder()
        builder.append(False)
        builder.append_values([True, False, True, False, True, False])
        builder.append(1)
        builder.append_null()
        arr = builder.finish()

        self.assertIsInstance(arr, Array)
        self.assertEqual(arr.null_count, 2)
        self.assertEqual(len(arr), 9)
        self.assertEqual(
            arr.to_pylist(), [False, True, False, True, False, True, False, None, None]
        )
        self.assertEqual(arr.type, self.data_type)


class TestBoolBuilder(TestCase, BoolBuilderTestMixin):
    def setUp(self):
        self.builder_cls = BoolBuilder
        self.data_type = bool_()


class TestCodeBuilder(TestCase):
    def test_simple(self):
        # Greetings in various languages, from
        # https://www.w3.org/2001/06/utf-8-test/UTF-8-demo.html
        values = ["Hello world", "Καλημέρα κόσμε", "コンニチハ"]
        values += ["hello\u0000world"]
        builder = CodeBuilder()
        builder.append(Code(values[0]))
        builder.append_values([Code(v) for v in values[1:]])
        builder.append("foo")
        builder.append_null()
        arr = builder.finish()

        codes = [Code(v) for v in values]

        self.assertIsInstance(arr, Array)
        self.assertEqual(arr.null_count, 2)
        self.assertEqual(len(arr), 6)
        self.assertEqual(arr.to_pylist(), codes + [None, None])


class TestDate32Builder(TestCase):
    def test_simple(self):
        values = [datetime(1970 + i, 1, 1) for i in range(3)]
        builder = Date32Builder()
        builder.append(values[0])
        builder.append_values(values[1:])
        builder.append(1)
        builder.append_null()
        arr = builder.finish()

        self.assertIsInstance(arr, Array)
        self.assertEqual(arr.null_count, 2)
        self.assertEqual(len(arr), 5)
        dates = [v.date() for v in values]
        self.assertEqual(arr.to_pylist(), dates + [None, None])


class TestDate64Builder(TestCase):
    def test_simple(self):
        values = [datetime(1970 + i, 1, 1) for i in range(3)]
        builder = Date64Builder()
        builder.append(values[0])
        builder.append_values(values[1:])
        builder.append(1)
        builder.append_null()
        arr = builder.finish()

        self.assertIsInstance(arr, Array)
        self.assertEqual(arr.null_count, 2)
        self.assertEqual(len(arr), 5)
        dates = [v.date() for v in values]
        self.assertEqual(arr.to_pylist(), dates + [None, None])
