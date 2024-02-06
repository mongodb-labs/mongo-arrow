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
import calendar
from datetime import date, datetime, timedelta
from unittest import TestCase

from bson import Binary, Code, Decimal128, ObjectId
from pyarrow import Array, bool_, field, int32, int64, list_, struct, timestamp

from pymongoarrow.lib import (
    BinaryBuilder,
    BoolBuilder,
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
from pymongoarrow.types import ObjectIdType


class IntBuildersTestMixin:
    def test_simple(self):
        builder = self.builder_cls()
        builder.append(0)
        builder.append_values([1, 2, 3, 4])
        builder.append_null()
        arr = builder.finish()

        self.assertIsInstance(arr, Array)
        self.assertEqual(arr.null_count, 1)
        self.assertEqual(len(arr), 6)
        self.assertEqual(arr.to_pylist(), [0, 1, 2, 3, 4, None])
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

    def _datetime_to_millis(self, dtm):
        """Convert datetime to milliseconds since epoch UTC.
        Vendored from bson."""
        if dtm.utcoffset() is not None:
            dtm = dtm - dtm.utcoffset()
        return int(calendar.timegm(dtm.timetuple()) * 1000 + dtm.microsecond // 1000)

    def _millis_only(self, dt):
        """Convert a datetime to millisecond resolution."""
        micros = (dt.microsecond // 1000) * 1000
        return dt.replace(microsecond=micros)

    def test_simple(self):
        self.maxDiff = None

        builder = DatetimeBuilder(dtype=timestamp("ms"))
        datetimes = [datetime.utcnow() + timedelta(days=k * 100) for k in range(5)]
        builder.append(self._datetime_to_millis(datetimes[0]))
        builder.append_values([self._datetime_to_millis(k) for k in datetimes[1:]])
        builder.append_null()
        arr = builder.finish()

        self.assertIsInstance(arr, Array)
        self.assertEqual(arr.null_count, 1)
        self.assertEqual(len(arr), len(datetimes) + 1)
        for actual, expected in zip(arr, datetimes + [None]):
            if actual.is_valid:
                self.assertEqual(actual.as_py(), self._millis_only(expected))
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
        builder.append(0.123)
        builder.append_values([1.234, 2.345, 3.456, 4.567])
        builder.append_null()
        arr = builder.finish()

        self.assertIsInstance(arr, Array)
        self.assertEqual(arr.null_count, 1)
        self.assertEqual(len(arr), 6)
        self.assertEqual(arr.to_pylist(), [0.123, 1.234, 2.345, 3.456, 4.567, None])


class TestObjectIdBuilder(TestCase):
    def test_simple(self):
        ids = [ObjectId() for i in range(5)]
        builder = ObjectIdBuilder()
        builder.append(ids[0].binary)
        builder.append_values([oid.binary for oid in ids[1:]])
        builder.append_null()
        arr = builder.finish()

        self.assertIsInstance(arr, Array)
        self.assertEqual(arr.null_count, 1)
        self.assertEqual(len(arr), 6)
        self.assertEqual(arr.to_pylist(), ids + [None])


class TestStringBuilder(TestCase):
    def test_simple(self):
        # Greetings in various languages, from
        # https://www.w3.org/2001/06/utf-8-test/UTF-8-demo.html
        values = ["Hello world", "Καλημέρα κόσμε", "コンニチハ"]
        values += ["hello\u0000world"]
        builder = StringBuilder()
        builder.append(values[0].encode("utf8"))
        builder.append_values(values[1:])
        builder.append_null()
        arr = builder.finish()

        self.assertIsInstance(arr, Array)
        self.assertEqual(arr.null_count, 1)
        self.assertEqual(len(arr), 5)
        self.assertEqual(arr.to_pylist(), values + [None])


class TestDocumentBuilder(TestCase):
    def test_simple(self):
        dtype = struct([field("a", int32()), field("b", bool_())])
        builder = DocumentBuilder(dtype)
        builder.append({"a": 1, "b": True})
        builder.append_values([{"a": 1, "b": False}, {"a": 2, "b": True}])
        builder.append_null()
        arr = builder.finish()

        self.assertIsInstance(arr, Array)
        self.assertEqual(arr.null_count, 1)
        self.assertEqual(len(arr), 4)
        self.assertEqual(arr.type, dtype)

    def test_nested(self):
        sub_struct = struct([field("c", bool_()), field("d", ObjectIdType())])
        dtype = struct([field("a", int32()), field("b", sub_struct)])
        builder = DocumentBuilder(dtype)
        builder.append({"a": 1, "b": {"c": True, "d": ObjectId()}})
        builder.append_values(
            [
                {"a": 1, "b": {"c": False, "d": ObjectId()}},
                {"a": 2, "b": {"c": True, "d": ObjectId()}},
                {"a": 3, "b": None},  # Null
                {"a": 4},  # Missing
                {"a": 5, "b": {}},  # Empty
                {"a": 6, "b": 1},  # Wrong type
                {"a": 6, "b": {"c": 1, "d": 1}},  # Wrong field types
            ]
        )
        builder.append_null()
        arr = builder.finish()

        self.assertIsInstance(arr, Array)
        self.assertEqual(arr.null_count, 1)
        self.assertEqual(len(arr), 9)


class TestListBuilder(TestCase):
    def test_simple(self):
        dtype = list_(int32())
        builder = ListBuilder(dtype)
        builder.append({"1": 1, "2": 3})
        builder.append_values(
            [
                {"1": 1, "2": 4},
                {"1": 2},
                {"1": None},  # Null
                None,
                {"a": 5, "b": 1},
            ]
        )
        arr = builder.finish()

        self.assertIsInstance(arr, Array)
        self.assertEqual(arr.null_count, 1)
        self.assertEqual(len(arr), 6)


class TestBinaryBuilder(TestCase):
    def test_simple(self):
        data = [Binary(bytes(i), 10) for i in range(5)]
        builder = BinaryBuilder(10)
        builder.append(data[0])
        builder.append_values(data[1:])
        builder.append_null()
        arr = builder.finish()

        self.assertIsInstance(arr, Array)
        self.assertEqual(arr.null_count, 1)
        self.assertEqual(len(arr), 6)
        self.assertEqual(arr.to_pylist(), data + [None])


class TestDecimal128Builder(TestCase):
    def test_simple(self):
        data = [Decimal128([i, i]) for i in range(5)]
        builder = Decimal128Builder()
        builder.append(data[0].bid)
        builder.append_values([item.bid for item in data[1:]])
        builder.append_null()
        arr = builder.finish()

        self.assertIsInstance(arr, Array)
        self.assertEqual(arr.null_count, 1)
        self.assertEqual(len(arr), 6)
        self.assertEqual(arr.to_pylist(), data + [None])


class BoolBuilderTestMixin:
    def test_simple(self):
        builder = BoolBuilder()
        builder.append(False)
        builder.append_values([True, False, True, False, True, False])
        builder.append_null()
        arr = builder.finish()

        self.assertIsInstance(arr, Array)
        self.assertEqual(arr.null_count, 1)
        self.assertEqual(len(arr), 8)
        self.assertEqual(arr.to_pylist(), [False, True, False, True, False, True, False, None])
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
        builder.append(values[0].encode("utf8"))
        builder.append_values(values[1:])
        builder.append_null()
        arr = builder.finish()

        codes = [Code(v) for v in values]

        self.assertIsInstance(arr, Array)
        self.assertEqual(arr.null_count, 1)
        self.assertEqual(len(arr), 5)
        self.assertEqual(arr.to_pylist(), codes + [None])


class TestDate32Builder(TestCase):
    def test_simple(self):
        epoch = date(1970, 1, 1)
        values = [date(2012, 1, 1), date(2012, 1, 2), date(2014, 4, 5)]
        builder = Date32Builder()
        builder.append(values[0].toordinal() - epoch.toordinal())
        builder.append_values([v.toordinal() - epoch.toordinal() for v in values[1:]])
        builder.append_null()
        arr = builder.finish()

        self.assertIsInstance(arr, Array)
        self.assertEqual(arr.null_count, 1)
        self.assertEqual(len(arr), 4)
        self.assertEqual(arr.to_pylist(), values + [None])


class TestDate64Builder(TestCase):
    def test_simple(self):
        def msec_since_epoch(d):
            epoch = datetime(1970, 1, 1)
            d = datetime.fromordinal(d.toordinal())
            diff = d - epoch
            return diff.total_seconds() * 1000

        values = [date(2012, 1, 1), date(2012, 1, 2), date(2014, 4, 5)]
        builder = Date64Builder()
        builder.append(msec_since_epoch(values[0]))
        builder.append_values([msec_since_epoch(v) for v in values[1:]])
        builder.append_null()
        arr = builder.finish()

        self.assertIsInstance(arr, Array)
        self.assertEqual(arr.null_count, 1)
        self.assertEqual(len(arr), 4)
        self.assertEqual(arr.to_pylist(), values + [None])
