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
# from datetime import datetime, timedelta
import unittest
import unittest.mock as mock
from test import client_context
from test.utils import AllowListEventListener

import numpy as np
import pandas as pd
from bson import Decimal128, ObjectId
from pyarrow import bool_, decimal128, float64, int32, int64, string, timestamp
from pymongo import DESCENDING, WriteConcern
from pymongo.collection import Collection
from pymongoarrow.api import Schema, aggregate_pandas_all, find_pandas_all, write
from pymongoarrow.errors import ArrowWriteError
from pymongoarrow.types import Decimal128StringType, ObjectIdType


class TestExplicitPandasApi(unittest.TestCase):
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

    def test_find_simple(self):
        expected = pd.DataFrame(data={"_id": [1, 2, 3, 4], "data": [10, 20, 30, None]}).astype(
            {"_id": "int32"}
        )

        table = find_pandas_all(self.coll, {}, schema=self.schema)
        self.assertTrue(table.equals(expected))

        expected = pd.DataFrame(data={"_id": [4, 3], "data": [None, 30]}).astype({"_id": "int32"})
        table = find_pandas_all(
            self.coll, {"_id": {"$gt": 2}}, schema=self.schema, sort=[("_id", DESCENDING)]
        )
        self.assertTrue(table.equals(expected))

        find_cmd = self.cmd_listener.results["started"][-1]
        self.assertEqual(find_cmd.command_name, "find")
        self.assertEqual(find_cmd.command["projection"], {"_id": True, "data": True})

    def test_aggregate_simple(self):
        expected = pd.DataFrame(data={"_id": [1, 2, 3, 4], "data": [20, 40, 60, None]}).astype(
            {"_id": "int32"}
        )
        projection = {"_id": True, "data": {"$multiply": [2, "$data"]}}
        table = aggregate_pandas_all(self.coll, [{"$project": projection}], schema=self.schema)
        self.assertTrue(table.equals(expected))

        agg_cmd = self.cmd_listener.results["started"][-1]
        self.assertEqual(agg_cmd.command_name, "aggregate")
        assert len(agg_cmd.command["pipeline"]) == 2
        self.assertEqual(agg_cmd.command["pipeline"][0]["$project"], projection)
        self.assertEqual(agg_cmd.command["pipeline"][1]["$project"], {"_id": True, "data": True})

    def round_trip(self, data, schema, coll=None):
        if coll is None:
            coll = self.coll
        coll.drop()
        res = write(self.coll, data)
        self.assertEqual(len(data), res.raw_result["insertedCount"])
        pd.testing.assert_frame_equal(data, find_pandas_all(coll, {}, schema=schema))
        return res

    def test_write_error(self):
        schema = {"_id": "int32", "data": "int64"}

        data = pd.DataFrame(
            data={"_id": [i for i in range(10001)] * 2, "data": [i * 2 for i in range(10001)] * 2}
        ).astype(schema)
        with self.assertRaises(ArrowWriteError):
            try:
                self.round_trip(data, Schema({"_id": int32(), "data": int64()}))
            except ArrowWriteError as awe:
                self.assertEqual(
                    10001, awe.details["writeErrors"][0]["index"], awe.details["nInserted"]
                )
                raise awe

    def test_write_schema_validation(self):
        schema = {
            "data": "int64",
            "float": "float64",
            "datetime": "datetime64[ms]",
            "string": "object",
            "bool": "bool",
        }
        data = pd.DataFrame(
            data={
                "data": [i for i in range(2)],
                "float": [i for i in range(2)],
                "datetime": [i for i in range(2)],
                "string": [str(i) for i in range(2)],
                "bool": [True for _ in range(2)],
            }
        ).astype(schema)
        self.round_trip(
            data,
            Schema(
                {
                    "data": int64(),
                    "float": float64(),
                    "datetime": timestamp("ms"),
                    "string": string(),
                    "bool": bool_(),
                }
            ),
        )

        schema = {"_id": "int32", "data": np.ubyte()}
        data = pd.DataFrame(
            data={"_id": [i for i in range(2)], "data": [i for i in range(2)]}
        ).astype(schema)
        with self.assertRaises(ValueError):
            self.round_trip(data, Schema({"_id": int32(), "data": decimal128(2)}))

    @mock.patch.object(Collection, "insert_many", side_effect=Collection.insert_many, autospec=True)
    def test_write_batching(self, mock):
        schema = {"_id": "int64"}
        data = pd.DataFrame(
            data={"_id": [i for i in range(100040)]},
        ).astype(schema)
        self.round_trip(
            data,
            Schema(
                {
                    "_id": int64(),
                }
            ),
            coll=self.coll,
        )
        self.assertEqual(mock.call_count, 2)


class TestBSONTypes(TestExplicitPandasApi):
    @classmethod
    def setUpClass(cls):
        TestExplicitPandasApi.setUpClass()
        cls.schema = Schema({"_id": ObjectIdType(), "decimal128": Decimal128StringType()})
        cls.coll = cls.client.pymongoarrow_test.get_collection(
            "test", write_concern=WriteConcern(w="majority")
        )
        cls.oids = [ObjectId() for i in range(4)]
        cls.decimal_128s = [Decimal128(i) for i in ["1.0", "0.1", "1e-5"]]

    def setUp(self):
        self.coll.drop()
        self.coll.insert_many(
            [
                {"_id": self.oids[0], "decimal128": self.decimal_128s[0]},
                {"_id": self.oids[1], "decimal128": self.decimal_128s[1]},
                {"_id": self.oids[2], "decimal128": self.decimal_128s[2]},
                {"_id": self.oids[3]},
            ]
        )
        self.cmd_listener.reset()
        self.getmore_listener.reset()

    def test_find_decimal128_pandas(self):
        decimals = [str(i) for i in self.decimal_128s] + [None]  # type:ignore
        pd_schema = {"_id": np.string_, "decimal128": np.object_}
        expected = pd.DataFrame(
            data={"_id": [i.binary for i in self.oids], "decimal128": decimals}
        ).astype(pd_schema)

        table = find_pandas_all(self.coll, {}, schema=self.schema)
        pd.testing.assert_frame_equal(expected, table)
