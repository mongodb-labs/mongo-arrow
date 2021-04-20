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
import unittest

import pytz

from bson.codec_options import CodecOptions

from pyarrow import int32, timestamp, schema as ArrowSchema, Table
from pymongo import WriteConcern, ASCENDING
from pymongoarrow.api import find_arrow_all, Schema
from pymongoarrow.context import PyMongoArrowContext

from test import client_context


class TestDateTimeType(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not client_context.connected:
            raise unittest.SkipTest("cannot connect to MongoDB")
        cls.client = client_context.get_client()
        cls.coll = cls.client.pymongoarrow_test.get_collection(
            'test', write_concern=WriteConcern(w='majority'))
        cls.coll.drop()
        cls.coll.insert_many([
            {'_id': 1, 'data': datetime.utcnow() + timedelta(milliseconds=10)},
            {'_id': 2, 'data': datetime.utcnow() + timedelta(milliseconds=25)}])
        cls.expected_times = []
        for doc in cls.coll.find({}, sort=[('_id', ASCENDING)]):
            cls.expected_times.append(doc['data'])

    def test_context_creation_fails_with_unsupported_granularity(self):
        unsupported_granularities = ['s', 'us', 'ns']
        for g in unsupported_granularities:
            schema = Schema({'_id': int32(), 'data': timestamp(g)})
            with self.assertRaises(TypeError):
                PyMongoArrowContext.from_schema(schema)

    def test_round_trip(self):
        expected = Table.from_pydict(
            {'_id': [1, 2], 'data': self.expected_times},
            ArrowSchema([('_id', int32()), ('data', timestamp('ms'))]))

        schemas = [Schema({'_id': int32(), 'data': timestamp('ms')}),
                   Schema({'_id': int32(), 'data': datetime})]
        for schema in schemas:
            table = find_arrow_all(
                self.coll, {}, schema=schema, sort=[('_id', ASCENDING)])
            self.assertEqual(table, expected)

    def test_timezone(self):
        # Specifying a timezone in the schema type specifier will cause
        # PyMongoArrow to interpret the corresponding timestamps in the
        # given timezone (by default, everything is assumed to be in UTC)
        tz = pytz.timezone('US/Pacific')
        expected = Table.from_pydict(
            {'_id': [1, 2], 'data': self.expected_times},
            ArrowSchema([('_id', int32()), ('data', timestamp('ms', tz=tz))]))

        schema = Schema({'_id': int32(), 'data': timestamp('ms', tz=tz)})
        table = find_arrow_all(
            self.coll, {}, schema=schema, sort=[('_id', ASCENDING)])
        self.assertEqual(table, expected)

    def test_timezone_codec_options(self):
        # 1. When specified, CodecOptions.tzinfo will modify timestamp
        #    type specifiers in the schema to inherit the specified timezone
        tz = pytz.timezone('US/Pacific')
        codec_options = CodecOptions(tz_aware=True, tzinfo=tz)
        expected = Table.from_pydict(
            {'_id': [1, 2], 'data': self.expected_times},
            ArrowSchema([('_id', int32()), ('data', timestamp('ms', tz=tz))]))

        schemas = [Schema({'_id': int32(), 'data': timestamp('ms')}),
                   Schema({'_id': int32(), 'data': datetime})]
        for schema in schemas:
            table = find_arrow_all(
                self.coll.with_options(codec_options=codec_options), {},
                schema=schema, sort=[('_id', ASCENDING)])

            self.assertEqual(table, expected)

        # 2. CodecOptions.tzinfo will be ignored when tzinfo is specified
        #    in the original schema type specifier.
        tz_east = pytz.timezone('US/Eastern')
        codec_options = CodecOptions(tz_aware=True, tzinfo=tz_east)
        schema = Schema({'_id': int32(), 'data': timestamp('ms', tz=tz)})
        table = find_arrow_all(
            self.coll.with_options(codec_options=codec_options), {},
            schema=schema, sort=[('_id', ASCENDING)])
        self.assertEqual(table, expected)
