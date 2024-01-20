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
from datetime import datetime, timedelta
from test import client_context
from test.utils import AllowListEventListener

import polars as pl
import pyarrow as pa
from bson import Decimal128, ObjectId
from polars.testing import assert_frame_equal
from pyarrow import (
    int32,
    int64,
)
from pymongo import DESCENDING, WriteConcern
from pymongo.collection import Collection

from pymongoarrow import api
from pymongoarrow.api import Schema, aggregate_polars_all, find_polars_all, write
from pymongoarrow.errors import ArrowWriteError
from pymongoarrow.types import _TYPE_NORMALIZER_FACTORY


class PolarsTestBase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not client_context.connected:
            raise unittest.SkipTest("cannot connect to MongoDB")
        cls.cmd_listener = AllowListEventListener("find", "aggregate")
        cls.getmore_listener = AllowListEventListener("getMore")
        cls.client = client_context.get_client(
            event_listeners=[cls.getmore_listener, cls.cmd_listener]
        )


class TestExplicitPolarsApi(PolarsTestBase):
    @classmethod
    def setUpClass(cls):
        PolarsTestBase.setUpClass()
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
        expected = pl.DataFrame(
            data={
                "_id": pl.Series(values=[1, 2, 3, 4], dtype=pl.Int32),
                "data": pl.Series(values=[10, 20, 30, None], dtype=pl.Int64),
            }
        )
        table = find_polars_all(self.coll, {}, schema=self.schema)
        self.assertEqual(expected.dtypes, table.dtypes)
        self.assertTrue(table.equals(expected))

        expected = pl.DataFrame(
            data={
                "_id": pl.Series(values=[4, 3], dtype=pl.Int32),
                "data": pl.Series(values=[None, 30], dtype=pl.Int64),
            }
        )
        table = find_polars_all(
            self.coll,
            {"_id": {"$gt": 2}},
            schema=self.schema,
            sort=[("_id", DESCENDING)],
        )
        self.assertEqual(expected.dtypes, table.dtypes)
        self.assertTrue(table.equals(expected))

        find_cmd = self.cmd_listener.results["started"][-1]
        self.assertEqual(find_cmd.command_name, "find")
        self.assertEqual(find_cmd.command["projection"], {"_id": True, "data": True})

    def test_aggregate_simple(self):
        expected = pl.DataFrame(
            data={
                "_id": pl.Series(values=[1, 2, 3, 4], dtype=pl.Int32),
                "data": pl.Series(values=[20, 40, 60, None], dtype=pl.Int64),
            }
        )
        projection = {"_id": True, "data": {"$multiply": [2, "$data"]}}
        table = aggregate_polars_all(self.coll, [{"$project": projection}], schema=self.schema)
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
            pl.testing.assert_series_equal(in_col, out_col)

    def round_trip(self, df_in, schema=None, coll=None):
        if coll is None:
            coll = self.coll
        coll.drop()
        res = write(self.coll, df_in)
        self.assertEqual(len(df_in), res.raw_result["insertedCount"])
        df_out = find_polars_all(coll, {}, schema=schema)
        self._assert_frames_equal(df_in, df_out)
        return res

    def test_datetime(self):
        """Test round trip of type: datetime"""
        n = 4
        expected = pl.DataFrame(
            data={
                "_id": pl.Series(values=range(n), dtype=pl.Int32),
                "data": pl.Series(
                    values=[
                        datetime(2024, 1, i) + timedelta(milliseconds=10) for i in range(1, n + 1)
                    ],
                    dtype=pl.Datetime(time_unit="ms"),
                ),
            }
        )
        self.round_trip(expected, schema=None)

    @mock.patch.object(Collection, "insert_many", side_effect=Collection.insert_many, autospec=True)
    def test_write_batching(self, mock):
        # todo - review how we now that call_count will be 2. Is N guaranteed to be large enough?
        data = pl.DataFrame(data={"_id": pl.Series(values=range(100040), dtype=pl.Int64)})
        self.round_trip(data, Schema(dict(_id=int64())), coll=self.coll)
        self.assertEqual(mock.call_count, 2)

    def test_write_error(self):
        """Confirm expected error is raised, simple duplicate key case."""
        n = 3
        data = pl.DataFrame(
            data={
                "_id": pl.Series(values=list(range(n)) * 2, dtype=pl.Int32),
                "data": pl.Series(values=range(n * 2), dtype=pl.Int64),
            }
        )
        with self.assertRaises(ArrowWriteError):
            try:
                self.round_trip(data, Schema({"_id": int32(), "data": int64()}))
            except ArrowWriteError as awe:
                self.assertEqual(n, awe.details["writeErrors"][0]["index"])
                self.assertEqual(n, awe.details["nInserted"])
                raise awe

    # TODO START HERE.
    #   *  We are creating two dataframes that are very similar. Can they be combined?
    #       Clarify their intent.
    #       - arrow table is to write all types and make sure we can round trip with find_polars_all
    #   * Add [None] to Series
    #   * Add tests of Struct  (and passing in dicts)
    #   * Retry the commented out attempt at Extension types in df below. Add test with exception.

    def create_dataframe(self):
        """First attempt to write to all PyMongoArrow types from similar Polars ones."""
        arrow_schema = {
            k.__name__: v(True)
            for k, v in _TYPE_NORMALIZER_FACTORY.items()
            # if k.__name__ not in ("ObjectId", "Decimal128", "Binary", "Code")
        }
        # The following was my first attempt to replace the extension types with their base types
        # arrow_schema['ObjectId'] = pa.binary(12)
        # arrow_schema['Binary'] = pa.binary()
        # arrow_schema["Code"] = pa.string()
        # arrow_schema["Decimal128"] = pa.decimal128(28)
        # arrow_schema["_id"] = int32()  # This is here as we currently require _id column in write()

        df = pl.DataFrame(
            data={
                # "_id": pl.Series(values=[i for i in range(2)] + [None], dtype=pl.Int32),
                "Int64": pl.Series(values=[i for i in range(2)] + [None], dtype=pl.Int64),
                "float": pl.Series(values=[i for i in range(2)] + [None], dtype=pl.Float64),
                "int": pl.Series(values=[i for i in range(2)] + [None], dtype=pl.Int64),
                "datetime": pl.Series(
                    values=[datetime(1970 + i, 1, 1) for i in range(2)] + [None],
                    dtype=pl.Datetime(time_unit="ms"),
                ),
                "str": pl.Series(values=[f"a{i}" for i in range(2)] + [None], dtype=pl.String),
                "bool": pl.Series(values=[True, False, None], dtype=pl.Boolean),
                # Extension Types
                # "ObjectId": pl.Series(values=[ObjectId().binary for i in range(2)] + [None], dtype=pl.Binary),
                # "Binary": pl.Series(values=[Binary(bytes(i), 10) for i in range(2)] + [None], dtype=pl.Binary),
                # "Decimal128": pl.Series(values=[10, 20, None], dtype=pl.Decimal(28)),
                # "Code": pl.Series(values=[Code(str(i)) for i in range(2)] + [None], dtype=pl.String)
            }
        )

        return arrow_schema, df

    def test_write_schema_validation(self):
        arrow_schema, df = self.create_dataframe()
        self.round_trip(df, Schema(arrow_schema))

    def test_auto_schema_succeeds_on_find(self):
        """Confirms Polars can read ObjectID Extension type.

        This is inserted automatically by Collection.insert_many
        Note that the output dtype is int32
        """
        vals = [1, "2", True, 4]
        data = [{"a": v} for v in vals]

        self.coll.drop()
        self.coll.insert_many(data)  # ObjectID autogenerated here

        df_out = find_polars_all(self.coll, {})
        self.assertEqual(df_out.columns, ["_id", "a"])
        self.assertEqual(df_out.shape, (4, 2))
        self.assertEqual(df_out.dtypes, [pl.Binary, pl.Int32])

    # 3a. Create test with all types supported by MongoDB
    # This tests api._arrow_to_polars, now casting to base Arrow types
    def _create_arrow_table(self):
        """Helper function creates n Arrow Table with all supported data types"""
        schema = {k.__name__: v(True) for k, v in _TYPE_NORMALIZER_FACTORY.items()}
        data = pa.Table.from_pydict(
            {
                "Int64": [i for i in range(2)],
                "float": [i for i in range(2)],
                "datetime": [i for i in range(2)],
                "str": [str(i) for i in range(2)],
                "int": [i for i in range(2)],
                "bool": [True, False],
                "Binary": [b"1", b"23"],
                "ObjectId": [ObjectId().binary, ObjectId().binary],
                "Decimal128": [Decimal128(str(i)).bid for i in range(2)],
                "Code": [str(i) for i in range(2)],
            },
            pa.schema(schema),
        )
        return Schema(schema), data

    def test_polars_succeeds_to_find_all_bson_types(self):
        """find_polars_all from collection written from an Arrow.Table"""
        self.coll.drop()
        arrow_schema, arrow_table_in = self._create_arrow_table()
        res = write(self.coll, arrow_table_in)
        self.assertEqual(len(arrow_table_in), res.raw_result["insertedCount"])
        df_out = find_polars_all(self.coll, query={}, schema=arrow_schema)

        # Now test_cast_away_extension_types_on_table
        arrow_cast = api._cast_away_extension_types_on_table(arrow_table_in)
        assert_frame_equal(df_out, pl.from_arrow(arrow_cast))

    def test_exceptions_for_unsupported_datatypes(self):
        """Confirms exceptions thrown are expected.
        Tracks future changes in any packages."""

        # Series:  PyMongoError does not support
        with self.assertRaises(ValueError) as exc:
            pls = pl.Series(values=range(2))
            write(self.coll, pls)
        self.assertTrue("Invalid tabular data object" in exc.exception.args[0])

        # Polars has an Object Type, similar in concept to Pandas
        class MyObject:
            pass

        with self.assertRaises(pl.PolarsPanicError) as exc:
            df_in = pl.DataFrame(data=[MyObject()] * 2)
            write(self.coll, df_in)
        self.assertTrue("not implemented" in exc.exception.args[0])

    def test_polars_binary_type(self):
        """Demonstrates roundtrip behavior of Polar Binary Type.

        It also shows that the automatically assigned _id column has this type.
        """
        # 1. _id added by MongoDB
        self.coll.drop()
        df_in = pl.DataFrame({"Binary": [b"1", b"one"]}, schema={"Binary": pl.Binary})
        write(self.coll, df_in)
        df_out = find_polars_all(self.coll, {})
        self.assertTrue(df_out.columns == ["_id", "Binary"])
        self.assertTrue(all([isinstance(c, pl.Binary) for c in df_out.dtypes]))
        self.assertTrue(assert_frame_equal(df_in, df_out.select("Binary")) is None)
        # 2. Explicit Binary _id
        self.coll.drop()
        df_in = pl.DataFrame(
            data=dict(_id=[b"0", b"1"], Binary=[b"1", b"one"]),
            schema=dict(_id=pl.Binary, Binary=pl.Binary),
        )
        write(self.coll, df_in)
        df_out = find_polars_all(self.coll, {})
        self.assertEqual(df_out.columns, ["_id", "Binary"])
        self.assertTrue(all([isinstance(c, pl.Binary) for c in df_out.dtypes]))
        self.assertTrue(assert_frame_equal(df_in, df_out) is None)
        # 3. Explicit Int32 _id
        self.coll.drop()
        df_in = pl.DataFrame(
            data={"_id": [0, 1], "Binary": [b"1", b"one"]},
            schema={"_id": pl.Int32, "Binary": pl.Binary},
        )
        write(self.coll, df_in)
        df_out = find_polars_all(self.coll, {})
        self.assertEqual(df_out.columns, ["_id", "Binary"])
        out_types = df_out.dtypes
        self.assertTrue(isinstance(out_types[0], pl.Int32))
        self.assertTrue(isinstance(out_types[1], pl.Binary))
        self.assertTrue(assert_frame_equal(df_in, df_out) is None)
