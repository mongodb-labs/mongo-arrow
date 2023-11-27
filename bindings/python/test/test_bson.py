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

import pyarrow as pa
from bson import Decimal128, Int64, InvalidBSON, encode

from pymongoarrow.context import PyMongoArrowContext
from pymongoarrow.lib import process_bson_stream
from pymongoarrow.schema import Schema
from pymongoarrow.types import ObjectId, ObjectIdType, int64, string


class TestBsonToArrowConversionBase(TestCase):
    def setUp(self):
        self.schema = Schema({"_id": ObjectId, "data": int64(), "title": string()})
        self.context = PyMongoArrowContext.from_schema(self.schema)

    @staticmethod
    def _generate_payload(doclist):
        payload = b""
        for doc in doclist:
            payload += encode(doc)
        return payload

    def _run_test(self, doclist, as_dict):
        payload = type(self)._generate_payload(doclist)

        process_bson_stream(payload, self.context)
        table = self.context.finish()
        table_dict = table.to_pydict()

        for key, value in table_dict.items():
            self.assertEqual(value, as_dict[key])


class TestValidBsonToArrowConversion(TestBsonToArrowConversionBase):
    def test_simple(self):
        ids = [ObjectId() for i in range(4)]
        docs = [
            {"_id": ids[0], "data": 10, "title": "ä"},
            {"_id": ids[1], "data": 20, "title": "b"},
            {"_id": ids[2], "data": 30, "title": "č"},
            {"_id": ids[3], "data": 40, "title": "ê"},
        ]
        as_dict = {
            "_id": ids,
            "data": [10, 20, 30, 40],
            "title": ["ä", "b", "č", "ê"],
        }

        self._run_test(docs, as_dict)

    def test_with_nulls(self):
        ids = [ObjectId() for i in range(4)]
        docs = [
            {"_id": ids[0], "data": 10, "title": "a"},
            {"_id": ids[1], "data": 20},
            {"_id": ids[2]},
            {"_id": ids[3], "data": 40},
            {"foo": 1},
            {},
        ]
        as_dict = {
            "_id": ids + [None, None],
            "data": [10, 20, None, 40, None, None],
            "title": ["a", None, None, None, None, None],
        }

        self._run_test(docs, as_dict)


class TestInvalidBsonToArrowConversion(TestBsonToArrowConversionBase):
    @staticmethod
    def _generate_payload(doclist):
        return TestBsonToArrowConversionBase._generate_payload(doclist)[:-2]

    def test_simple(self):
        ids = [ObjectId() for i in range(4)]
        docs = [
            {"_id": ids[0], "data": 10},
            {"_id": ids[1], "data": 20},
            {"_id": ids[2], "data": 30},
            {"_id": ids[3], "data": 40},
        ]
        as_dict = {"_id": [oid.binary for oid in ids], "data": [10, 20, 30, 40]}

        with self.assertRaisesRegex(InvalidBSON, "Could not read BSON document stream"):
            self._run_test(docs, as_dict)


class TestUnsupportedDataType(TestBsonToArrowConversionBase):
    def test_simple(self):
        schema = Schema({"_id": ObjectId, "data": int64(), "fake": pa.float16()})
        msg = 'Unsupported data type in schema for field "fake" of type "halffloat"'
        with self.assertRaisesRegex(ValueError, msg):
            PyMongoArrowContext.from_schema(schema)


class TestNonAsciiFieldName(TestBsonToArrowConversionBase):
    def setUp(self):
        self.schema = Schema({"_id": ObjectIdType(), "dätá": int64()})
        self.context = PyMongoArrowContext.from_schema(self.schema)

    def test_simple(self):
        ids = [ObjectId() for i in range(4)]
        docs = [
            {"_id": ids[0], "dätá": 10},
            {"_id": ids[1], "dätá": 20},
            {"_id": ids[2], "dätá": 30},
            {"_id": ids[3], "dätá": 40},
        ]
        as_dict = {"_id": ids, "dätá": [10, 20, 30, 40]}

        self._run_test(docs, as_dict)


class TestSerializeExtensions(TestCase):
    # Follows example in
    # https://arrow.apache.org/docs/python/extending_types.html#defining-extension-types-user-defined-types

    def serialize_array(self, arr):
        batch = pa.RecordBatch.from_arrays([arr], ["ext"])
        sink = pa.BufferOutputStream()
        with pa.RecordBatchStreamWriter(sink, batch.schema) as writer:
            writer.write_batch(batch)
        buf = sink.getvalue()
        with pa.ipc.open_stream(buf) as reader:
            result = reader.read_all()
        return result.column("ext")

    def test_object_id_type(self):
        oids = [ObjectId().binary for _ in range(4)]
        storage_array = pa.array(oids, pa.binary(12))
        arr = pa.ExtensionArray.from_storage(ObjectIdType(), storage_array)
        result = self.serialize_array(arr)
        assert result.type._type_marker == ObjectIdType._type_marker


class TestInt64Type(TestBsonToArrowConversionBase):
    def setUp(self):
        self.schema = Schema({"data": Int64})
        self.context = PyMongoArrowContext.from_schema(self.schema)

    def test_simple(self):
        docs = [
            {"data": Int64(1e15)},
            {"data": Int64(-1e5)},
            {"data": Int64(10)},
        ]
        as_dict = {"data": [1e15, -1e5, 10]}
        self._run_test(docs, as_dict)


class TestBooleanType(TestBsonToArrowConversionBase):
    def setUp(self):
        self.schema = Schema({"data": bool})
        self.context = PyMongoArrowContext.from_schema(self.schema)

    def test_simple(self):
        docs = [
            {"data": True},
            {"data": False},
            {"data": 19},
            {"data": "string"},
            {"data": False},
            {"data": True},
        ]
        as_dict = {"data": [True, False, None, None, False, True]}
        self._run_test(docs, as_dict)


class TestStringType(TestBsonToArrowConversionBase):
    def setUp(self):
        self.schema = Schema({"data": str})
        self.context = PyMongoArrowContext.from_schema(self.schema)

    def test_simple(self):
        docs = [
            {"data": "a"},
            {"data": "dätá"},
        ]
        as_dict = {"data": ["a", "dätá"]}
        self._run_test(docs, as_dict)


class TestDecimal128Type(TestBsonToArrowConversionBase):
    def setUp(self):
        self.schema = Schema({"data": Decimal128})
        self.context = PyMongoArrowContext.from_schema(self.schema)

    def test_simple(self):
        docs = [
            {"data": Decimal128("0.01")},
            {"data": Decimal128("1e-5")},
            {"data": Decimal128("1.0e+5")},
        ]
        as_dict = {"data": [Decimal128(i) for i in ["0.01", "0.00001", "1.0E+5"]]}
        self._run_test(docs, as_dict)


class TestSubdocumentType(TestBsonToArrowConversionBase):
    def setUp(self):
        self.schema = Schema({"data": dict(x=bool)})
        self.context = PyMongoArrowContext.from_schema(self.schema)

    def test_simple(self):
        docs = [
            {"data": dict(x=True)},
            {"data": dict(x=False)},
            {"data": dict(x=19)},
            {"data": dict(x="string")},
            {"data": dict(x=False)},
            {"data": dict(x=True)},
        ]
        as_dict = {
            "data": [
                dict(x=True),
                dict(x=False),
                dict(x=None),
                dict(x=None),
                dict(x=False),
                dict(x=True),
            ]
        }
        self._run_test(docs, as_dict)

    def test_nested(self):
        self.schema = Schema({"data": dict(x=bool, y=dict(a=int))})
        self.context = PyMongoArrowContext.from_schema(self.schema)

        docs = [
            {"data": dict(x=True, y=dict(a=1))},
            {"data": dict(x=False, y=dict(a=1))},
            {"data": dict(x=19, y=dict(a=1))},
            {"data": dict(x="string", y=dict(a=1))},
            {"data": dict(x=False, y=dict(a=1))},
            {"data": dict(x=True, y=dict(a=1))},
        ]
        as_dict = {
            "data": [
                dict(x=True, y=dict(a=1)),
                dict(x=False, y=dict(a=1)),
                dict(x=None, y=dict(a=1)),
                dict(x=None, y=dict(a=1)),
                dict(x=False, y=dict(a=1)),
                dict(x=True, y=dict(a=1)),
            ]
        }
        self._run_test(docs, as_dict)
