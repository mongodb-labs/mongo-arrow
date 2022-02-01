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

from bson import encode, InvalidBSON

import pyarrow
from pymongoarrow.context import PyMongoArrowContext
from pymongoarrow.lib import process_bson_stream
from pymongoarrow.schema import Schema
from pymongoarrow.types import int32, int64, string, ObjectId, ObjectIdType


class TestBsonToArrowConversionBase(TestCase):
    def setUp(self):
        self.schema = Schema({'_id': ObjectId,
                              'data': int64(),
                              'title': string() })
        self.context = PyMongoArrowContext.from_schema(
            self.schema)

    @staticmethod
    def _generate_payload(doclist):
        payload = b''
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
        docs = [{'_id': ids[0], 'data': 10, 'title': 'ä'},
                {'_id': ids[1], 'data': 20, 'title': 'b'},
                {'_id': ids[2], 'data': 30, 'title': 'č'},
                {'_id': ids[3], 'data': 40, 'title': 'ê'}]
        as_dict = {
            '_id': [oid.binary for oid in ids] ,
            'data': [10, 20, 30, 40],
            'title': ['ä', 'b', 'č', 'ê']}

        self._run_test(docs, as_dict)

    def test_with_nulls(self):
        ids = [ObjectId() for i in range(4)]
        docs = [{'_id': ids[0], 'data': 10, 'title': 'a'},
                {'_id': ids[1], 'data': 20},
                {'_id': ids[2]},
                {'_id': ids[3], 'data': 40},
                {'foo': 1},
                {}]
        as_dict = {
            '_id': [oid.binary for oid in ids] + [None, None],
            'data': [10, 20, None, 40, None, None],
            'title': ['a', None, None, None, None, None] }

        self._run_test(docs, as_dict)


class TestInvalidBsonToArrowConversion(TestBsonToArrowConversionBase):
    @staticmethod
    def _generate_payload(doclist):
        return TestBsonToArrowConversionBase._generate_payload(
            doclist)[:-2]

    def test_simple(self):
        ids = [ObjectId() for i in range(4)]
        docs = [{'_id': ids[0], 'data': 10},
                {'_id': ids[1], 'data': 20},
                {'_id': ids[2], 'data': 30},
                {'_id': ids[3], 'data': 40}]
        as_dict = {
            '_id': [oid.binary for oid in ids],
            'data': [10, 20, 30, 40]}

        with self.assertRaisesRegex(
                InvalidBSON, "Could not read BSON document stream"):
            self._run_test(docs, as_dict)


class TestUnsupportedDataType(TestBsonToArrowConversionBase):

    def test_simple(self):
        schema = Schema({'_id': ObjectId,
                         'data': int64(),
                         'fake':  pyarrow.float16() })
        msg =  ("Unsupported data type in schema for field " +
                '"fake" of type "halffloat"')
        with self.assertRaisesRegex(ValueError, msg):
            PyMongoArrowContext.from_schema(schema)


class TestNonAsciiFieldName(TestBsonToArrowConversionBase):

    def setUp(self):
        self.schema = Schema({'_id': ObjectIdType(),
                              'dätá': int64()})
        self.context = PyMongoArrowContext.from_schema(
            self.schema)

    def test_simple(self):
        ids = [ObjectId() for i in range(4)]
        docs = [{'_id': ids[0], 'dätá': 10 },
                {'_id': ids[1], 'dätá': 20 },
                {'_id': ids[2], 'dätá': 30 },
                {'_id': ids[3], 'dätá': 40 }]
        as_dict = {
            '_id': [oid.binary for oid in ids] ,
            'dätá': [10, 20, 30, 40]
        }

        self._run_test(docs, as_dict)
