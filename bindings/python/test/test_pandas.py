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
import datetime
import tempfile
import unittest
import unittest.mock as mock
import warnings
from test import client_context
from test.utils import AllowListEventListener, NullsTestMixin

import numpy as np
import pandas as pd
import pandas.testing
import pyarrow as pa
from bson import Binary, Code, CodecOptions, Decimal128, ObjectId
from pyarrow import decimal256, int32, int64
from pymongo import DESCENDING, WriteConcern
from pymongo.collection import Collection
from pytz import timezone

from pymongoarrow.api import Schema, aggregate_pandas_all, find_pandas_all, write
from pymongoarrow.errors import ArrowWriteError
from pymongoarrow.pandas_types import PandasBSONDtype, PandasDecimal128, PandasObjectId
from pymongoarrow.types import _TYPE_NORMALIZER_FACTORY, Decimal128Type, ObjectIdType


class PandasTestBase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not client_context.connected:
            raise unittest.SkipTest("cannot connect to MongoDB")
        cls.cmd_listener = AllowListEventListener("find", "aggregate")
        cls.getmore_listener = AllowListEventListener("getMore")
        cls.client = client_context.get_client(
            event_listeners=[cls.getmore_listener, cls.cmd_listener]
        )


class TestExplicitPandasApi(PandasTestBase):
    @classmethod
    def setUpClass(cls):
        PandasTestBase.setUpClass()
        cls.schema = Schema({"_id": int32(), "data": int64()})
        cls.coll = cls.client.pymongoarrow_test.get_collection(
            "test", write_concern=WriteConcern(w="majority")
        )

    def setUp(self):
        self.coll.drop()
        self.coll.insert_many(
            [
                {"_id": 1, "data": 10},
                {"_id": 2, "data": 20},
                {"_id": 3, "data": 30},
                {"_id": 4},
            ]
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
            self.coll,
            {"_id": {"$gt": 2}},
            schema=self.schema,
            sort=[("_id", DESCENDING)],
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

    def _assert_frames_equal(self, incoming, outgoing):
        for name in incoming.columns:
            in_col = incoming[name]
            out_col = outgoing[name]
            # Object types may lose type information in a round trip.
            # Integer types with missing values are converted to floating
            # point in a round trip.
            if str(out_col.dtype) in ["object", "float64", "datetime64[ms]"]:
                out_col = out_col.astype(in_col.dtype)
            pd.testing.assert_series_equal(in_col, out_col)

    def round_trip(self, data, schema, coll=None):
        if coll is None:
            coll = self.coll
        coll.drop()
        res = write(self.coll, data)
        self.assertEqual(len(data), res.raw_result["insertedCount"])
        self._assert_frames_equal(data, find_pandas_all(coll, {}, schema=schema))
        return res

    def test_write_error(self):
        schema = {"_id": "int32", "data": "int64"}

        data = pd.DataFrame(
            data={
                "_id": [i for i in range(10001)] * 2,
                "data": [i * 2 for i in range(10001)] * 2,
            }
        ).astype(schema)
        with self.assertRaises(ArrowWriteError):
            try:
                self.round_trip(data, Schema({"_id": int32(), "data": int64()}))
            except ArrowWriteError as awe:
                self.assertEqual(
                    10001,
                    awe.details["writeErrors"][0]["index"],
                    awe.details["nInserted"],
                )
                raise awe

    def _create_data(self):
        arrow_schema = {
            k.__name__: v(True) if k != Binary else v(10)
            for k, v in _TYPE_NORMALIZER_FACTORY.items()
        }
        schema = {k: v.to_pandas_dtype() for k, v in arrow_schema.items()}
        schema["Int64"] = pd.Int64Dtype()
        schema["int"] = pd.Int32Dtype()
        schema["str"] = "string"
        schema["datetime"] = "datetime64[ms]"

        data = pd.DataFrame(
            data={
                "ObjectId": [ObjectId() for i in range(2)] + [None],
                "Int64": [i for i in range(2)] + [None],
                "float": [i for i in range(2)] + [None],
                "int": [i for i in range(2)] + [None],
                "datetime": [datetime.datetime(1970 + i, 1, 1) for i in range(2)] + [None],
                "str": [f"a{i}" for i in range(2)] + [None],
                "bool": [True, False, None],
                "Binary": [Binary(bytes(i), 10) for i in range(2)] + [None],
                "Decimal128": [Decimal128(str(i)) for i in range(2)] + [None],
                "Code": [Code(str(i)) for i in range(2)] + [None],
            }
        ).astype(schema)
        return arrow_schema, data

    def test_write_schema_validation(self):
        arrow_schema, data = self._create_data()

        # Work around https://github.com/pandas-dev/pandas/issues/16248,
        # Where pandas does not implement utcoffset for null timestamps.
        def new_replace(k):
            if isinstance(k, pd.NaT.__class__):
                return datetime.datetime(1970, 1, 1)
            return k.replace(tzinfo=None)

        data["datetime"] = data.apply(lambda row: new_replace(row["datetime"]), axis=1)

        self.round_trip(
            data,
            Schema(arrow_schema),
        )

        schema = {"_id": "int32", "data": bytes}
        data = pd.DataFrame(
            data={"_id": [i for i in range(2)], "data": [i for i in range(2)]}
        ).astype(schema)
        with self.assertRaises(ValueError):
            self.round_trip(data, Schema({"_id": int32(), "data": decimal256(2)}))

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

    def test_string_bool(self):
        schema = {
            "string": "str",
            "bool": "bool",
        }
        data = pd.DataFrame(
            data=[{"string": [str(i) for i in range(2)], "bool": [True for _ in range(2)]}],
        ).astype(schema)

        self.round_trip(
            data,
            Schema(
                {
                    "string": str,
                    "bool": bool,
                }
            ),
        )

    def _create_nested_data(self):
        schema = {
            "string": "string",
            "bool": "bool",
            "double": "float64",
            "int32": "int32",
            "dt": "datetime64[ms]",
        }
        schema["nested"] = "object"

        raw_data = {
            "string": [str(i) for i in range(3)],
            "bool": [True for _ in range(3)],
            "double": [0.1 for _ in range(3)],
            "int32": [i for i in range(3)],
            "dt": [datetime.datetime(1970 + i, 1, 1) for i in range(3)],
        }

        def inner(i):
            return dict(
                string=str(i),
                bool=bool(i),
                double=i + 0.1,
                int32=i,
                dt=datetime.datetime(1970 + i, 1, 1),
            )

        raw_data["nested"] = [inner(i) for i in range(3)]
        return pd.DataFrame(data=raw_data).astype(schema)

    def test_auto_schema(self):
        data = self._create_nested_data()
        self.coll.drop()
        res = write(self.coll, data)
        self.assertEqual(len(data), res.raw_result["insertedCount"])
        for func in [find_pandas_all, aggregate_pandas_all]:
            out = func(self.coll, {} if func == find_pandas_all else []).drop(columns=["_id"])
            for name in data.columns:
                val = out[name]
                if str(val.dtype) == "object":
                    val = val.astype(data[name].dtype)
                pd.testing.assert_series_equal(data[name], val)

    def test_auto_schema_heterogeneous(self):
        vals = [1, "2", True, 4]
        data = [{"a": v} for v in vals]

        self.coll.drop()
        self.coll.insert_many(data)
        for func in [find_pandas_all, aggregate_pandas_all]:
            out = func(self.coll, {} if func == find_pandas_all else []).drop(columns=["_id"])
            np.equal(out["a"], [1.0, np.nan, 1, 4.0])

    def test_auto_schema_tz(self):
        schema = {
            "bool": "bool",
            "dt": "datetime64[ms, US/Eastern]",
            "string": "str",
        }
        data = pd.DataFrame(
            data={
                "string": [None] + [str(i) for i in range(2)],
                "bool": [True for _ in range(3)],
                "dt": [
                    datetime.datetime(1970 + i, 1, 1, tzinfo=timezone("US/Eastern"))
                    for i in range(3)
                ],
            },
        ).astype(schema)
        self.coll.drop()
        codec_options = CodecOptions(tzinfo=timezone("US/Eastern"), tz_aware=True)
        res = write(self.coll.with_options(codec_options=codec_options), data)
        self.assertEqual(len(data), res.raw_result["insertedCount"])
        for func in [find_pandas_all, aggregate_pandas_all]:
            out = func(
                self.coll.with_options(codec_options=codec_options),
                {} if func == find_pandas_all else [],
            ).drop(columns=["_id"])
            pd.testing.assert_frame_equal(data, out)

    def test_csv(self):
        # Pandas csv does not support nested data.
        # cf https://github.com/pandas-dev/pandas/issues/40652
        _, data = self._create_data()
        for name in data.columns.to_list():
            if isinstance(data[name].dtype, PandasBSONDtype):
                data = data.drop(labels=[name], axis=1)

        with tempfile.NamedTemporaryFile(suffix=".csv") as f:
            f.close()
            # May give RuntimeWarning due to the nulls.
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", RuntimeWarning)
                data.to_csv(f.name, index=False, na_rep="")
            out = pd.read_csv(f.name)
            self._assert_frames_equal(data, out)


class TestBSONTypes(PandasTestBase):
    @classmethod
    def setUpClass(cls):
        PandasTestBase.setUpClass()
        cls.schema = Schema({"_id": ObjectIdType(), "decimal128": Decimal128Type()})
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

    def test_find_decimal128(self):
        decimals = self.decimal_128s + [None]
        pd_schema = {"_id": PandasObjectId(), "decimal128": PandasDecimal128()}
        expected = pd.DataFrame(data={"_id": self.oids, "decimal128": decimals}).astype(pd_schema)

        table = find_pandas_all(self.coll, {}, schema=self.schema)
        pd.testing.assert_frame_equal(expected, table)


class TestNulls(NullsTestMixin, unittest.TestCase):
    def find_fn(self, coll, query, schema):
        return find_pandas_all(coll, query, schema=schema)

    def equal_fn(self, left, right):
        left = left.fillna(0)
        right = right.fillna(0)
        if type(left) == pandas.DataFrame:
            pandas.testing.assert_frame_equal(left, right, check_dtype=False)
        else:
            pandas.testing.assert_series_equal(left, right, check_dtype=False)

    def table_from_dict(self, dict, schema=None):
        return pandas.DataFrame(dict)

    def assert_in_idx(self, table, col_name):
        self.assertTrue(col_name in table.columns)

    pytype_tab_map = {
        str: "object",
        int: ["int64", "float64"],
        float: "float64",
        datetime.datetime: "datetime64[ms]",
        ObjectId: "bson_PandasObjectId",
        Decimal128: "bson_PandasDecimal128",
        bool: "object",
    }

    pytype_writeback_exc_map = {
        str: None,
        int: None,
        float: None,
        datetime.datetime: ValueError,
        ObjectId: pa.lib.ArrowInvalid,
        Decimal128: pa.lib.ArrowInvalid,
        bool: None,
    }

    def na_safe(self, atype):
        return True
