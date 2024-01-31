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
from pyarrow import Table, int32, timestamp
from pyarrow import schema as ArrowSchema
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
        """Test behavior of setting tzinfo CodecOptions in Collection.with_options.

        When provided, timestamp type specifiers in the schema to inherit the specified timezone.
        Read values will maintain this information for timestamps whether schema is passed or not.

        Note, this does not apply to datetimes.
        We also test here that if one asks for a different timezone upon reading,
        on returns the requested timezone.
        """

        # 1. We pass tzinfo to Collection.with_options, and same tzinfo in schema of find_arrow_all
        tz_west = pytz.timezone("US/Pacific")
        codec_options = CodecOptions(tz_aware=True, tzinfo=tz_west)
        coll_west = self.coll.with_options(codec_options=codec_options)

        schema_west = ArrowSchema([("_id", int32()), ("data", timestamp("ms", tz=tz_west))])
        table_west = find_arrow_all(
            collection=coll_west,
            query={},
            schema=Schema.from_arrow(schema_west),
            sort=[("_id", ASCENDING)],
        )

        expected_west = Table.from_pydict(
            {"_id": [1, 2], "data": self.expected_times}, schema=schema_west
        )
        self.assertTrue(table_west.equals(expected_west))

        # 2. We pass tzinfo to Collection.with_options, but do NOT include a schema in find_arrow_all
        table_none = find_arrow_all(
            collection=coll_west,
            query={},
            schema=None,
            sort=[("_id", ASCENDING)],
        )
        self.assertTrue(table_none.equals(expected_west))

        # 3. Now we pass a DIFFERENT timezone to the schema in find_arrow_all than we did to the Collection
        schema_east = Schema(
            {"_id": int32(), "data": timestamp("ms", tz=pytz.timezone("US/Eastern"))}
        )
        table_east = find_arrow_all(
            collection=coll_west,
            query={},
            schema=schema_east,
            sort=[("_id", ASCENDING)],
        )
        # Confirm that we get the timezone we requested
        self.assertTrue(table_east.schema.types == [int32(), timestamp(unit="ms", tz="US/Eastern")])
        # Confirm that the times have been adjusted
        times_west = table_west["data"].to_pylist()
        times_east = table_east["data"].to_pylist()
        self.assertTrue(all([times_west[i] == times_east[i] for i in range(len(table_east))]))

        # 4. Test behavior of datetime. Output will be pyarrow.timestamp("ms") without timezone
        schema_dt = Schema({"_id": int32(), "data": datetime})
        table_dt = find_arrow_all(
            collection=coll_west,
            query={},
            schema=schema_dt,
            sort=[("_id", ASCENDING)],
        )
        self.assertTrue(table_dt.schema.types == [int32(), timestamp(unit="ms")])
        times = table_dt["data"].to_pylist()
        self.assertTrue(times == self.expected_times)
