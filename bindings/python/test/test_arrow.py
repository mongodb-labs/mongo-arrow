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
import os
import unittest
import unittest.mock as mock
from test import client_context
from test.utils import AllowListEventListener

import pymongo
from bson import Decimal128, ObjectId
from pyarrow import Table, binary, bool_, decimal256, float64, int32, int64
from pyarrow import schema as ArrowSchema
from pyarrow import string, timestamp
from pyarrow.parquet import read_table, write_table
from pymongo import DESCENDING, WriteConcern
from pymongo.collection import Collection
from pymongoarrow.api import Schema, aggregate_arrow_all, find_arrow_all, write
from pymongoarrow.errors import ArrowWriteError
from pymongoarrow.monkey import patch_all
from pymongoarrow.types import Decimal128StringType, ObjectIdType


class TestArrowApiMixin:
    @classmethod
    def setUpClass(cls):
        if not client_context.connected:
            raise unittest.SkipTest("cannot connect to MongoDB")
        cls.cmd_listener = AllowListEventListener("find", "aggregate")
        cls.getmore_listener = AllowListEventListener("getMore")
        cls.client = client_context.get_client(
            event_listeners=[cls.getmore_listener, cls.cmd_listener]
        )
        cls.schema = Schema({"_id": int32(), "data": int64()})
        cls.coll = cls.client.pymongoarrow_test.get_collection(
            "test", write_concern=WriteConcern(w="majority")
        )

    def setUp(self):
        self.coll.drop()
        self.coll.insert_many(
            [{"_id": 1, "data": 10}, {"_id": 2, "data": 20}, {"_id": 3, "data": 30}, {"_id": 4}]
        )
        self.cmd_listener.reset()
        self.getmore_listener.reset()

    def run_find(self, *args, **kwargs):
        raise NotImplementedError

    def run_aggregate(self, *args, **kwargs):
        raise NotImplementedError

    def test_find_simple(self):
        expected = Table.from_pydict(
            {"_id": [1, 2, 3, 4], "data": [10, 20, 30, None]},
            ArrowSchema([("_id", int32()), ("data", int64())]),
        )
        table = self.run_find({}, schema=self.schema)
        self.assertEqual(table, expected)

        expected = Table.from_pydict(
            {"_id": [4, 3], "data": [None, 30]}, ArrowSchema([("_id", int32()), ("data", int64())])
        )
        table = self.run_find({"_id": {"$gt": 2}}, schema=self.schema, sort=[("_id", DESCENDING)])
        self.assertEqual(table, expected)

        find_cmd = self.cmd_listener.results["started"][-1]
        self.assertEqual(find_cmd.command_name, "find")
        self.assertEqual(find_cmd.command["projection"], {"_id": True, "data": True})

    def test_find_with_projection(self):
        expected = Table.from_pydict(
            {"_id": [4, 3], "data": [None, 60]}, ArrowSchema([("_id", int32()), ("data", int64())])
        )
        projection = {"_id": True, "data": {"$multiply": [2, "$data"]}}
        table = self.run_find(
            {"_id": {"$gt": 2}},
            schema=self.schema,
            sort=[("_id", DESCENDING)],
            projection=projection,
        )
        self.assertEqual(table, expected)

        find_cmd = self.cmd_listener.results["started"][-1]
        self.assertEqual(find_cmd.command_name, "find")
        self.assertEqual(find_cmd.command["projection"], projection)

    def test_find_with_session(self):
        if self.client.topology_description.topology_type_name == "Single":
            raise unittest.SkipTest("Unsupported topology")

        with self.client.start_session() as session:
            self.assertIsNone(session.operation_time)
            _ = session._server_session.last_use
            expected = Table.from_pydict(
                {"_id": [1, 2, 3, 4], "data": [10, 20, 30, None]},
                ArrowSchema([("_id", int32()), ("data", int64())]),
            )
            table = self.run_find({}, schema=self.schema, session=session)
            self.assertEqual(table, expected)
            self.assertIsNotNone(session.operation_time)

    def test_find_multiple_batches(self):
        orig_method = self.coll.find_raw_batches

        def mock_find_raw_batches(*args, **kwargs):
            kwargs["batch_size"] = 2
            return orig_method(*args, **kwargs)

        with mock.patch.object(
            pymongo.collection.Collection, "find_raw_batches", wraps=mock_find_raw_batches
        ):
            expected = Table.from_pydict(
                {"_id": [1, 2, 3, 4], "data": [10, 20, 30, None]},
                ArrowSchema([("_id", int32()), ("data", int64())]),
            )
            table = self.run_find({}, schema=self.schema)
            self.assertEqual(table, expected)
        self.assertGreater(len(self.getmore_listener.results["started"]), 1)

    def test_find_omits_id_if_not_in_schema(self):
        schema = Schema({"data": int64()})
        expected = Table.from_pydict({"data": [30, 20, 10, None]}, ArrowSchema([("data", int64())]))

        table = self.run_find({}, schema=schema, sort=[("data", DESCENDING)])
        self.assertEqual(table, expected)

        find_cmd = self.cmd_listener.results["started"][0]
        self.assertEqual(find_cmd.command_name, "find")
        self.assertFalse(find_cmd.command["projection"]["_id"])

    def test_aggregate_simple(self):
        expected = Table.from_pydict(
            {"_id": [1, 2, 3, 4], "data": [20, 40, 60, None]},
            ArrowSchema([("_id", int32()), ("data", int64())]),
        )
        projection = {"_id": True, "data": {"$multiply": [2, "$data"]}}
        table = self.run_aggregate([{"$project": projection}], schema=self.schema)
        self.assertEqual(table, expected)

        agg_cmd = self.cmd_listener.results["started"][-1]
        self.assertEqual(agg_cmd.command_name, "aggregate")
        assert len(agg_cmd.command["pipeline"]) == 2
        self.assertEqual(agg_cmd.command["pipeline"][0]["$project"], projection)
        self.assertEqual(agg_cmd.command["pipeline"][1]["$project"], {"_id": True, "data": True})

    def test_aggregate_multiple_batches(self):
        orig_method = self.coll.aggregate_raw_batches

        def mock_agg_raw_batches(*args, **kwargs):
            kwargs["batchSize"] = 2
            return orig_method(*args, **kwargs)

        with mock.patch.object(
            pymongo.collection.Collection, "aggregate_raw_batches", wraps=mock_agg_raw_batches
        ):
            expected = Table.from_pydict(
                {"_id": [4, 3, 2, 1], "data": [None, 30, 20, 10]},
                ArrowSchema([("_id", int32()), ("data", int64())]),
            )
            table = self.run_aggregate([{"$sort": {"_id": -1}}], schema=self.schema)
            self.assertEqual(table, expected)
        self.assertGreater(len(self.getmore_listener.results["started"]), 1)

    def test_aggregate_omits_id_if_not_in_schema(self):
        schema = Schema({"data": int64()})
        expected = Table.from_pydict({"data": [30, 20, 10, None]}, ArrowSchema([("data", int64())]))

        table = self.run_aggregate([{"$sort": {"data": DESCENDING}}], schema=schema)
        self.assertEqual(table, expected)

        agg_cmd = self.cmd_listener.results["started"][0]
        self.assertEqual(agg_cmd.command_name, "aggregate")
        for stage in agg_cmd.command["pipeline"]:
            op_name = next(iter(stage))
            if op_name == "$project":
                self.assertFalse(stage[op_name]["_id"])
                break

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
        self.round_trip(data, Schema(schema))

        schema = {"_id": int32(), "data": decimal256(2)}
        data = Table.from_pydict(
            {"_id": [i for i in range(2)], "data": [i for i in range(2)]},
            ArrowSchema(schema),
        )
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
        self.round_trip(data, Schema(schema), coll=self.coll)
        self.assertEqual(mock.call_count, 2)

    def test_parquet(self):
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
        write_table(data, "test.parquet")
        data = read_table("test.parquet")
        self.round_trip(data, Schema(schema))
        os.remove("test.parquet")


class TestArrowExplicitApi(TestArrowApiMixin, unittest.TestCase):
    def run_find(self, *args, **kwargs):
        return find_arrow_all(self.coll, *args, **kwargs)

    def run_aggregate(self, *args, **kwargs):
        return aggregate_arrow_all(self.coll, *args, **kwargs)


class TestArrowPatchedApi(TestArrowApiMixin, unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        patch_all()
        super().setUpClass()

    def run_find(self, *args, **kwargs):
        return self.coll.find_arrow_all(*args, **kwargs)

    def run_aggregate(self, *args, **kwargs):
        return self.coll.aggregate_arrow_all(*args, **kwargs)


class TestBSONTypes(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not client_context.connected:
            raise unittest.SkipTest("cannot connect to MongoDB")
        cls.cmd_listener = AllowListEventListener("find", "aggregate")
        cls.getmore_listener = AllowListEventListener("getMore")
        cls.client = client_context.get_client(
            event_listeners=[cls.getmore_listener, cls.cmd_listener]
        )

    def test_find_decimal128(self):
        oids = list(ObjectId() for i in range(4))
        decs = [Decimal128(i) for i in ["0.1", "1.0", "1e-5"]]
        schema = Schema({"_id": ObjectIdType(), "data": Decimal128StringType()})
        expected = Table.from_pydict(
            {
                "_id": [i.binary for i in oids],
                "data": [str(decs[0]), str(decs[1]), str(decs[2]), None],
            },
            ArrowSchema([("_id", binary(12)), ("data", string())]),
        )
        coll = self.client.pymongoarrow_test.get_collection(
            "test", write_concern=WriteConcern(w="majority")
        )

        coll.drop()
        coll.insert_many(
            [
                {"_id": oids[0], "data": decs[0]},
                {"_id": oids[1], "data": decs[1]},
                {"_id": oids[2], "data": decs[2]},
                {"_id": oids[3]},
            ]
        )
        table = find_arrow_all(coll, {}, schema=schema)
        self.assertEqual(table, expected)
