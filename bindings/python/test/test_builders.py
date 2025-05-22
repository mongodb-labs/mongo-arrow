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
    def test_simple_allow_invalid(self):
        builder = self.builder_cls(allow_invalid=True)
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

    def test_simple(self):
        builder = self.builder_cls()
        builder.append(0)
        builder.append_values([1, 2, 3, 4])
        with self.assertRaisesRegex(
            TypeError, f"Got unexpected type `string` instead of expected type `{self.data_type}`"
        ):
            builder.append("a")


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

    def test_simple_allow_invalid(self):
        self.maxDiff = None

        builder = DatetimeBuilder(dtype=timestamp("ms"), allow_invalid=True)
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

    def test_simple(self):
        self.maxDiff = None

        builder = DatetimeBuilder(dtype=timestamp("ms"))
        datetimes = [datetime.now(timezone.utc) + timedelta(days=k * 100) for k in range(5)]
        builder.append(datetimes[0])
        builder.append_values(datetimes[1:])
        with self.assertRaisesRegex(
            TypeError, "Got unexpected type `int32` instead of expected type `datetime`"
        ):
            builder.append(1)

    def test_unsupported_units(self):
        for unit in ("s", "us", "ns"):
            with self.assertRaises(TypeError):
                DatetimeBuilder(dtype=timestamp(unit))


class TestDoubleBuilder(TestCase):
    def test_simple_allow_invalid(self):
        builder = DoubleBuilder(allow_invalid=True)
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

    def test_simple(self):
        builder = DoubleBuilder()
        values = [0.123, 1.234, 2.345, 3.456, 4.567, 1]
        builder.append(values[0])
        builder.append_values(values[1:])
        with self.assertRaisesRegex(
            TypeError, "Got unexpected type `string` instead of expected type `double`"
        ):
            builder.append("a")


class TestObjectIdBuilder(TestCase):
    def test_simple_allow_invalid(self):
        ids = [ObjectId() for _ in range(5)]
        builder = ObjectIdBuilder(allow_invalid=True)
        builder.append(ids[0])
        builder.append_values(ids[1:])
        builder.append(b"123456789123")
        builder.append_null()
        arr = builder.finish()

        self.assertIsInstance(arr, Array)
        self.assertEqual(arr.null_count, 2)
        self.assertEqual(len(arr), 7)
        self.assertEqual(arr.to_pylist(), ids + [None, None])

    def test_simple(self):
        ids = [ObjectId() for _ in range(5)]
        builder = ObjectIdBuilder()
        builder.append(ids[0])
        builder.append_values(ids[1:])
        with self.assertRaisesRegex(
            TypeError, "Got unexpected type `binary` instead of expected type `objectId`"
        ):
            builder.append(b"123456789123")


class TestStringBuilder(TestCase):
    def test_simple_allow_invalid(self):
        # Greetings in various languages, from
        # https://www.w3.org/2001/06/utf-8-test/UTF-8-demo.html
        values = ["Hello world", "Καλημέρα κόσμε", "コンニチハ"]
        values += ["hello\u0000world"]
        builder = StringBuilder(allow_invalid=True)
        builder.append(values[0])
        builder.append_values(values[1:])
        builder.append(b"1")
        builder.append_null()
        arr = builder.finish()

        self.assertIsInstance(arr, Array)
        self.assertEqual(arr.null_count, 2)
        self.assertEqual(len(arr), 6)
        self.assertEqual(arr.to_pylist(), values + [None, None])

    def test_simple(self):
        # Greetings in various languages, from
        # https://www.w3.org/2001/06/utf-8-test/UTF-8-demo.html
        values = ["Hello world", "Καλημέρα κόσμε", "コンニチハ"]
        values += ["hello\u0000world"]
        builder = StringBuilder()
        builder.append(values[0])
        builder.append_values(values[1:])
        with self.assertRaisesRegex(
            TypeError, "Got unexpected type `binary` instead of expected type `string`"
        ):
            builder.append(b"1")


class TestDocumentBuilder(TestCase):
    def test_simple(self):
        builder = DocumentBuilder()
        builder.append(dict(a=1, b=2, c=3))
        builder.add_field(b"a")
        builder.add_field(b"b")
        builder.add_field(b"c")
        builder.append_null()
        builder.append(dict(a=1, b=2))
        builder.add_field(b"a")
        builder.add_field(b"b")
        names = builder.finish()
        assert names == ["a", "b", "c"]


class TestListBuilder(TestCase):
    def test_simple(self):
        builder = ListBuilder()
        builder.append([1, 2])
        builder.append_count()
        builder.append_count()
        builder.append_null()
        builder.append([3, 4, 5])
        builder.append_count()
        builder.append_count()
        builder.append_count()
        builder.append_null()
        arr = builder.finish()
        assert arr.to_pylist() == [0, None, 2, None, 5]


class TestBuilderManager(TestCase):
    def test_simple(self):
        manager = BuilderManager({}, False, None, False)
        data = b"".join(encode(d) for d in [dict(a=1), dict(a=2), dict(a=None), dict(a=4)])
        manager.process_bson_stream(data, len(data))
        array_map = manager.finish()
        assert list(array_map) == ["a"]
        assert next(iter(array_map.values())).finish().to_pylist() == [1, 2, None, 4]

    def test_nested_object(self):
        inner_values = []
        for i in range(3):
            inner_values.append(dict(a=i, b="1", c=None, d=[1.4], e=ObjectId(), f=None))
        values = []
        for i in range(3):
            values.append(dict(c=inner_values[i], e=ObjectId(), f=None, g=[dict(a=1)]))
        values.append(dict(c=None))
        inner = inner_values[0].copy()
        inner["c"] = 1.0
        values.append(dict(c=inner, e=ObjectId(), f=None, g=[]))
        manager = BuilderManager({}, False, None, False)
        data = b"".join(encode(v) for v in values)
        manager.process_bson_stream(data, len(data))
        array_map = manager.finish()
        for key, value in array_map.items():
            array_map[key] = value.finish()
        assert sorted(array_map.keys()) == [
            "c",
            "c.a",
            "c.b",
            "c.c",
            "c.d",
            "c.d[]",
            "c.e",
            "c.f",
            "e",
            "f",
            "g",
            "g[]",
            "g[].a",
        ]
        # Dict has its top level keys.
        assert array_map["c"] == ["a", "b", "c", "d", "e", "f"]
        # Deferred nested field.
        assert array_map["c.c"].to_pylist() == [None, None, None, None, 1.0]
        assert array_map["f"].to_pylist() == [None, None, None, None, None]
        # List with a null in the middle.
        assert array_map["c.d"].to_pylist() == [0, 1, 2, None, 3, 4]
        assert array_map["c.d[]"].to_pylist() == [1.4, 1.4, 1.4, 1.4]
        # Regular item with a null in the middle.
        assert array_map["c.b"].to_pylist() == ["1", "1", "1", None, "1"]
        # Nested object ids are object ids.
        obj = array_map["c.e"].to_pylist()[0]
        assert isinstance(obj, ObjectId)
        # Lists can contain objects.
        assert array_map["g[].a"].to_pylist() == [1, 1, 1]


class TestBinaryBuilder(TestCase):
    def test_simple_allow_invalid(self):
        data = [Binary(bytes(i), 10) for i in range(5)]
        builder = BinaryBuilder(10, allow_invalid=True)
        builder.append(data[0])
        builder.append_values(data[1:])
        builder.append(1)
        builder.append_null()
        arr = builder.finish()

        self.assertIsInstance(arr, Array)
        self.assertEqual(arr.null_count, 2)
        self.assertEqual(len(arr), 7)
        self.assertEqual(arr.to_pylist(), data + [None, None])

    def test_simple(self):
        data = [Binary(bytes(i), 10) for i in range(5)]
        builder = BinaryBuilder(10)
        builder.append(data[0])
        builder.append_values(data[1:])
        with self.assertRaisesRegex(
            TypeError, "Got unexpected type `int32` instead of expected type `binary`"
        ):
            builder.append(1)


class TestDecimal128Builder(TestCase):
    def test_simple_allow_invalid(self):
        data = [Decimal128([i, i]) for i in range(5)]
        builder = Decimal128Builder(allow_invalid=True)
        builder.append(data[0])
        builder.append_values(data[1:])
        builder.append(1)
        builder.append_null()
        arr = builder.finish()

        self.assertIsInstance(arr, Array)
        self.assertEqual(arr.null_count, 2)
        self.assertEqual(len(arr), 7)
        self.assertEqual(arr.to_pylist(), data + [None, None])

    def test_simple(self):
        data = [Decimal128([i, i]) for i in range(5)]
        builder = Decimal128Builder()
        builder.append(data[0])
        builder.append_values(data[1:])
        with self.assertRaisesRegex(
            TypeError, "Got unexpected type `int32` instead of expected type `decimal128`"
        ):
            builder.append(1)


class TestBoolBuilder(TestCase):
    def test_simple_allow_invalid(self):
        builder = BoolBuilder(allow_invalid=True)
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
        self.assertEqual(arr.type, bool_())

    def test_simple(self):
        builder = BoolBuilder()
        builder.append(False)
        builder.append_values([True, False, True, False, True, False])
        with self.assertRaisesRegex(
            TypeError, "Got unexpected type `int32` instead of expected type `boolean`"
        ):
            builder.append(1)


class TestCodeBuilder(TestCase):
    def test_simple_allow_invalid(self):
        # Greetings in various languages, from
        # https://www.w3.org/2001/06/utf-8-test/UTF-8-demo.html
        values = ["Hello world", "Καλημέρα κόσμε", "コンニチハ"]
        values += ["hello\u0000world"]
        builder = CodeBuilder(allow_invalid=True)
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

    def test_simple(self):
        # Greetings in various languages, from
        # https://www.w3.org/2001/06/utf-8-test/UTF-8-demo.html
        values = ["Hello world", "Καλημέρα κόσμε", "コンニチハ"]
        values += ["hello\u0000world"]
        builder = CodeBuilder()
        builder.append(Code(values[0]))
        builder.append_values([Code(v) for v in values[1:]])
        with self.assertRaisesRegex(
            TypeError, "Got unexpected type `string` instead of expected type `code`"
        ):
            builder.append("foo")


class TestDate32Builder(TestCase):
    def test_simple_allow_invalid(self):
        values = [datetime(1970 + i, 1, 1) for i in range(3)]
        builder = Date32Builder(allow_invalid=True)
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

    def test_simple(self):
        values = [datetime(1970 + i, 1, 1) for i in range(3)]
        builder = Date32Builder()
        builder.append(values[0])
        builder.append_values(values[1:])
        with self.assertRaisesRegex(
            TypeError, "Got unexpected type `int32` instead of expected type `date32`"
        ):
            builder.append(1)


class TestDate64Builder(TestCase):
    def test_simple_allow_invalid(self):
        values = [datetime(1970 + i, 1, 1) for i in range(3)]
        builder = Date64Builder(allow_invalid=True)
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

    def test_simple(self):
        values = [datetime(1970 + i, 1, 1) for i in range(3)]
        builder = Date64Builder()
        builder.append(values[0])
        builder.append_values(values[1:])
        with self.assertRaisesRegex(
            TypeError, "Got unexpected type `int32` instead of expected type `date64`"
        ):
            builder.append(1)
