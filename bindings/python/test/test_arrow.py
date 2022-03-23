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
import unittest.mock as mock
from test import client_context
from test.utils import AllowListEventListener

import pymongo
from pyarrow import Table, int32, int64
from pyarrow import schema as ArrowSchema
from pymongo import DESCENDING, WriteConcern
from pymongoarrow.api import Schema, aggregate_arrow_all, find_arrow_all, write
from pymongoarrow.monkey import patch_all


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

    def round_trip(self, data, schema):
        self.coll.drop()
        write(self.coll, data)
        self.assertEqual(data, find_arrow_all(self.coll, {}, schema=schema))

    def test_roundtrip(self):
        schema = Schema({"_id": int32(), "data": int64()})
        data = Table.from_pydict(
            {"_id": [i for i in range(10000)], "data": [i * 2 for i in range(10000)]},
            ArrowSchema([("_id", int32()), ("data", int64())]),
        )
        self.round_trip(data, schema)


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
