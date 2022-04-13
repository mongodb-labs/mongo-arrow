# Copyright 2022-present MongoDB, Inc.
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

import os
import unittest
import unittest.mock as mock
from test import client_context

import pyarrow.parquet as pq
from pyarrow import Table, bool_, decimal128, float64, int32, int64
from pyarrow import schema as ArrowSchema
from pyarrow import string, timestamp
from pymongo import WriteConcern
from pymongo.collection import Collection
from pymongoarrow.api import Schema, aggregate_arrow_all, find_arrow_all, write
from pymongoarrow.errors import ArrowWriteError
from pymongoarrow.monkey import patch_all


class TestParquetAPIMixin:
    @classmethod
    def setUpClass(cls):
        if not client_context.connected:
            raise unittest.SkipTest("cannot connect to MongoDB")
        cls.client = client_context.get_client()
        cls.coll = cls.client.pymongoarrow_test.get_collection(
            "test", write_concern=WriteConcern(w="majority")
        )

    def setUp(self):
        self.coll.drop()

    def tearDown(self):
        os.remove("table.parquet")

    def run_find(self, *args, **kwargs):
        raise NotImplementedError

    def run_aggregate(self, *args, **kwargs):
        raise NotImplementedError

    def round_trip(self, data, schema, coll=None):
        if coll is None:
            coll = self.coll
        self.coll.drop()
        res = write(self.coll, data)
        self.assertEqual(len(data), res.raw_result["insertedCount"])
        self.assertEqual(data, find_arrow_all(coll, {}, schema=schema))
        return res

    def test_write_error(self):
        schema = {"_id": int32(), "data": int64()}
        data = Table.from_pydict(
            {"_id": [i for i in range(10001)] * 2, "data": [i * 2 for i in range(10001)] * 2},
            ArrowSchema(schema),
        )
        pq.write_table(data, "table.parquet")
        data = pq.read_table("table.parquet")
        with self.assertRaises(ArrowWriteError):
            try:
                self.round_trip(data, Schema(schema))
            except ArrowWriteError as awe:
                self.assertEqual(
                    10001, awe.details["writeErrors"][0]["index"], awe.details["nInserted"]
                )
                raise awe

    def test_write_schema_validation(self):
        schema = {
            "data": int64(),
            "float": float64(),
            "datetime": timestamp("ms"),
            "string": string(),
            "bool": bool_(),
        }
        data = Table.from_pydict(
            {
                "data": [i for i in range(2)],
                "float": [i for i in range(2)],
                "datetime": [i for i in range(2)],
                "string": [str(i) for i in range(2)],
                "bool": [True for _ in range(2)],
            },
            ArrowSchema(schema),
        )
        pq.write_table(data, "table.parquet")
        data = pq.read_table("table.parquet")
        self.round_trip(data, Schema(schema))

        schema = {"_id": int32(), "data": decimal128(2)}
        data = Table.from_pydict(
            {"_id": [i for i in range(2)], "data": [i for i in range(2)]},
            ArrowSchema(schema),
        )
        pq.write_table(data, "table.parquet")
        data = pq.read_table("table.parquet")
        with self.assertRaises(ValueError):
            self.round_trip(data, Schema(schema))

    @mock.patch.object(Collection, "insert_many", side_effect=Collection.insert_many, autospec=True)
    def test_write_batching(self, mock):
        schema = {
            "_id": int64(),
        }
        data = Table.from_pydict(
            {"_id": [i for i in range(100040)]},
            ArrowSchema(schema),
        )
        pq.write_table(data, "table.parquet")
        data = pq.read_table("table.parquet")
        self.round_trip(data, Schema(schema), coll=self.coll)
        self.assertEqual(mock.call_count, 2)


class TestParquetExplicitApi(TestParquetAPIMixin, unittest.TestCase):
    def run_find(self, *args, **kwargs):
        return find_arrow_all(self.coll, *args, **kwargs)

    def run_aggregate(self, *args, **kwargs):
        return aggregate_arrow_all(self.coll, *args, **kwargs)


class TestParquetPatchedApi(TestParquetAPIMixin, unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        patch_all()
        super().setUpClass()

    def run_find(self, *args, **kwargs):
        return self.coll.find_arrow_all(*args, **kwargs)

    def run_aggregate(self, *args, **kwargs):
        return self.coll.aggregate_arrow_all(*args, **kwargs)
