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
import unittest
from datetime import datetime, timedelta
from test import client_context

import pytz
from bson.codec_options import CodecOptions
from pyarrow import Table, int32
from pyarrow import schema as ArrowSchema
from pyarrow import timestamp
from pymongo import ASCENDING, WriteConcern
from pymongoarrow.api import Schema, find_arrow_all
from pymongoarrow.context import PyMongoArrowContext


class TestDateTimeType(unittest.TestCase):
    def setUp(self):
        if not client_context.connected:
            raise unittest.SkipTest("cannot connect to MongoDB")
        self.client = client_context.get_client()
        self.coll = self.client.pymongoarrow_test.get_collection(
            "test", write_concern=WriteConcern(w="majority")
        )
        self.coll.drop()
        self.coll.insert_many(
            [
                {"_id": 1, "data": datetime.utcnow() + timedelta(milliseconds=10)},
                {"_id": 2, "data": datetime.utcnow() + timedelta(milliseconds=25)},
            ]
        )
        self.expected_times = []
        for doc in self.coll.find({}, sort=[("_id", ASCENDING)]):
            self.expected_times.append(doc["data"])

    def test_context_creation_fails_with_unsupported_granularity(self):
        unsupported_granularities = ["s", "us", "ns"]
        for g in unsupported_granularities:
            schema = Schema({"_id": int32(), "data": timestamp(g)})
            with self.assertRaises(TypeError):
                PyMongoArrowContext.from_schema(schema)

    def test_round_trip(self):
        expected = Table.from_pydict(
            {"_id": [1, 2], "data": self.expected_times},
            ArrowSchema([("_id", int32()), ("data", timestamp("ms"))]),
        )

        schemas = [
            Schema({"_id": int32(), "data": timestamp("ms")}),
            Schema({"_id": int32(), "data": datetime}),
        ]
        for schema in schemas:
            table = find_arrow_all(self.coll, {}, schema=schema, sort=[("_id", ASCENDING)])
            self.assertEqual(table, expected)

    def test_timezone_round_trip(self):
        # Timezone-aware datetimes written by PyMongo can be read back as
        # Arrow datetimes with the appropriate timezone metadata
        tz = pytz.timezone("US/Pacific")
        expected_times = [k.astimezone(tz=tz) for k in self.expected_times]
        self.coll.drop()
        self.coll.insert_many(
            [{"_id": idx, "data": data} for idx, data in enumerate(expected_times)]
        )
        expected = Table.from_pydict(
            {"_id": [0, 1], "data": expected_times},
            ArrowSchema([("_id", int32()), ("data", timestamp("ms", tz=tz))]),
        )

        schema = Schema({"_id": int32(), "data": timestamp("ms", tz=tz)})
        table = find_arrow_all(self.coll, {}, schema=schema, sort=[("_id", ASCENDING)])
        self.assertEqual(table, expected)

    def test_timezone_specified_in_schema(self):
        # Specifying a timezone in the schema type specifier will cause
        # PyMongoArrow to interpret the corresponding timestamps in the
        # given timezone (everything in Mongo is in UTC)
        tz = pytz.timezone("US/Pacific")
        expected = Table.from_pydict(
            {"_id": [1, 2], "data": self.expected_times},
            ArrowSchema([("_id", int32()), ("data", timestamp("ms", tz=tz))]),
        )

        schema = Schema({"_id": int32(), "data": timestamp("ms", tz=tz)})
        table = find_arrow_all(self.coll, {}, schema=schema, sort=[("_id", ASCENDING)])
        self.assertEqual(table, expected)

    def test_timezone_specified_in_codec_options(self):
        # 1. When specified, CodecOptions.tzinfo will modify timestamp
        #    type specifiers in the schema to inherit the specified timezone
        tz = pytz.timezone("US/Pacific")
        codec_options = CodecOptions(tz_aware=True, tzinfo=tz)
        expected = Table.from_pydict(
            {"_id": [1, 2], "data": self.expected_times},
            ArrowSchema([("_id", int32()), ("data", timestamp("ms", tz=tz))]),
        )

        schemas = [
            Schema({"_id": int32(), "data": timestamp("ms")}),
            Schema({"_id": int32(), "data": datetime}),
        ]
        for schema in schemas:
            table = find_arrow_all(
                self.coll.with_options(codec_options=codec_options),
                {},
                schema=schema,
                sort=[("_id", ASCENDING)],
            )

            self.assertEqual(table, expected)

        # 2. CodecOptions.tzinfo will be ignored when tzinfo is specified
        #    in the original schema type specifier.
        tz_east = pytz.timezone("US/Eastern")
        codec_options = CodecOptions(tz_aware=True, tzinfo=tz_east)
        schema = Schema({"_id": int32(), "data": timestamp("ms", tz=tz)})
        table = find_arrow_all(
            self.coll.with_options(codec_options=codec_options),
            {},
            schema=schema,
            sort=[("_id", ASCENDING)],
        )
        self.assertEqual(table, expected)
