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
import uuid
from datetime import datetime
from test import client_context
from test.utils import AllowListEventListener

import bson
import polars as pl
import pyarrow as pa
from polars.testing import assert_frame_equal
from pyarrow import int32, int64
from pymongo import DESCENDING, WriteConcern
from pymongo.collection import Collection

from pymongoarrow import api
from pymongoarrow.api import Schema, aggregate_polars_all, find_arrow_all, find_polars_all, write
from pymongoarrow.errors import ArrowWriteError
from pymongoarrow.types import (
    _TYPE_NORMALIZER_FACTORY,
    BinaryType,
    CodeType,
    Decimal128Type,
    ObjectIdType,
)


class PolarsTestBase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not client_context.connected:
            raise unittest.SkipTest("cannot connect to MongoDB")
        cls.cmd_listener = AllowListEventListener("find", "aggregate")
        cls.getmore_listener = AllowListEventListener("getMore")
        cls.client = client_context.get_client(
            event_listeners=[cls.getmore_listener, cls.cmd_listener],
            uuidRepresentation="standard",
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
        """Insert simple use case data."""
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

    def round_trip(self, df_in, schema=None, **kwargs):
        """Helper tests pl.DataFrame written matches that found."""
        self.coll.drop()
        res = write(self.coll, df_in)
        self.assertEqual(len(df_in), res.raw_result["insertedCount"])
        df_out = find_polars_all(self.coll, {}, schema=schema, **kwargs)
        pl.testing.assert_frame_equal(df_in, df_out)
        return res

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

    @mock.patch.object(Collection, "insert_many", side_effect=Collection.insert_many, autospec=True)
    def test_write_batching(self, mock):
        data = pl.DataFrame(data={"_id": pl.Series(values=range(100040), dtype=pl.Int64)})
        self.round_trip(data, Schema(dict(_id=int64())))
        self.assertEqual(mock.call_count, 2)

    def test_duplicate_key_error(self):
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

    def test_polars_types(self):
        """Test round-trip of DataFrame consisting of Polar.DataTypes.

        This does NOT include ExtensionTypes as Polars doesn't support them (yet).
        """
        pl_typenames = ["Int64", "Int32", "Float64", "Datetime", "String", "Boolean"]
        pl_types = [pl.Int64, pl.Int32, pl.Float64, pl.Datetime("ms"), pl.String, pl.Boolean]
        pa_types = [
            pa.int64(),
            pa.int32(),
            pa.float64(),
            pa.timestamp("ms"),
            pa.string(),
            pa.bool_(),
        ]

        pl_schema = dict(zip(pl_typenames, pl_types))
        pa_schema = dict(zip(pl_typenames, pa_types))

        data = {
            "Int64": pl.Series([i for i in range(2)] + [None]),
            "Int32": pl.Series([i for i in range(2)] + [None]),
            "Float64": pl.Series([i for i in range(2)] + [None]),
            "Datetime": pl.Series([datetime(1970 + i, 1, 1) for i in range(2)] + [None]),
            "String": pl.Series([f"a{i}" for i in range(2)] + [None]),
            "Boolean": pl.Series([True, False, None]),
        }

        df_in = pl.DataFrame._from_dict(data=data, schema=pl_schema)
        self.coll.drop()
        write(self.coll, df_in)
        df_out = find_polars_all(self.coll, {}, schema=Schema(pa_schema))
        pl.testing.assert_frame_equal(df_in, df_out.drop("_id"))

    def test_extension_types_fail(self):
        """Confirm failure on ExtensionTypes for Polars.DataFrame.from_arrow"""

        for ext_type, data in (
            (ObjectIdType(), [bson.ObjectId().binary, bson.ObjectId().binary]),
            (Decimal128Type(), [bson.Decimal128(str(i)).bid for i in range(2)]),
            (CodeType(), [str(i) for i in range(2)]),
        ):
            table = pa.Table.from_pydict({"foo": data}, pa.schema({"foo": ext_type}))
            with self.assertRaises(pl.exceptions.ComputeError):
                pl.from_arrow(table)

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

    def test_arrow_to_polars(self):
        """Test reading Polars data from written Arrow Data."""
        arrow_schema = {k.__name__: v(True) for k, v in _TYPE_NORMALIZER_FACTORY.items()}
        arrow_table_in = pa.Table.from_pydict(
            {
                "Int64": [i for i in range(2)],
                "float": [i for i in range(2)],
                "datetime": [i for i in range(2)],
                "str": [str(i) for i in range(2)],
                "int": [i for i in range(2)],
                "bool": [True, False],
                "Binary": [b"1", b"23"],
                "ObjectId": [bson.ObjectId().binary, bson.ObjectId().binary],
                "Decimal128": [bson.Decimal128(str(i)).bid for i in range(2)],
                "Code": [str(i) for i in range(2)],
            },
            pa.schema(arrow_schema),
        )

        self.coll.drop()
        res = write(self.coll, arrow_table_in)
        self.assertEqual(len(arrow_table_in), res.raw_result["insertedCount"])
        df_out = find_polars_all(self.coll, query={}, schema=Schema(arrow_schema))

        # Sanity check: compare with cast_away_extension_types_on_table
        arrow_cast = api._cast_away_extension_types_on_table(arrow_table_in)
        assert_frame_equal(df_out, pl.from_arrow(arrow_cast))

    def test_exceptions_for_unsupported_polar_types(self):
        """Confirm exceptions thrown are expected.

        Currently, pl.Series, and pl.Object
        Tracks future changes in any packages.
        """

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
        """Demonstrates that binary data is not yet supported. TODO [ARROW-214]

        Will demonstrate roundtrip behavior of Polar Binary Type.
        """
        # 1. _id added by MongoDB
        self.coll.drop()
        with self.assertRaises(ValueError):
            df_in = pl.DataFrame({"Binary": [b"1", b"one"]}, schema={"Binary": pl.Binary})
            write(self.coll, df_in)
            df_out = find_polars_all(self.coll, {})
            self.assertTrue(df_out.columns == ["_id", "Binary"])
            self.assertTrue(all([isinstance(c, pl.Binary) for c in df_out.dtypes]))
            self.assertIsNone(assert_frame_equal(df_in, df_out.select("Binary")))
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
            self.assertIsNone(assert_frame_equal(df_in, df_out))
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

    def test_bson_types(self):
        """Test reading Polars and Arrow data from written BSON Data.

        This is meant to capture the use case of reading data in Python
        that has been written to a DB from another language.

        Note that this tests only types currently supported by Arrow.
        bson.Regex is not included, for example.
        """

        # 1. Use pymongo / bson packages to build create and write tabular data
        self.coll.drop()
        collection = self.coll

        data_type_map = [
            {"type": "int", "value": 42, "atype": pa.int32(), "ptype": pl.Int32},
            {"type": "long", "value": 1234567890123456789, "atype": pa.int64(), "ptype": pl.Int64},
            {"type": "double", "value": 10.5, "atype": pa.float64(), "ptype": pl.Float64},
            {"type": "string", "value": "hello world", "atype": pa.string(), "ptype": pl.String},
            {"type": "boolean", "value": True, "atype": pa.bool_(), "ptype": pl.Boolean},
            {
                "type": "date",
                "value": datetime(2025, 1, 21),
                "atype": pa.timestamp("ms"),
                "ptype": pl.Datetime,
            },
            {
                "type": "object",
                "value": {"a": 1, "b": 2},
                "atype": pa.struct({"a": pa.int32(), "b": pa.int32()}),
                "ptype": pl.Struct({"a": pl.Int32, "b": pl.Int32}),
            },
            {
                "type": "array",
                "value": [1, 2, 3],
                "atype": pa.list_(pa.int32()),
                "ptype": pl.List(pl.Int32),
            },
            {
                "type": "bytes",
                "value": b"\x00\x01\x02\x03\x04",
                "atype": BinaryType(pa.binary()),
                "ptype": pl.Binary,
            },
            {
                "type": "binary data",
                "value": bson.Binary(b"\x00\x01\x02\x03\x04"),
                "atype": BinaryType(pa.binary()),
                "ptype": pl.Binary,
            },
            {
                "type": "object id",
                "value": bson.ObjectId(),
                "atype": ObjectIdType(),
                "ptype": pl.Object,
            },
            {
                "type": "javascript",
                "value": bson.Code("function() { return x; }"),
                "atype": CodeType(),
                "ptype": pl.String,
            },
            {
                "type": "decimal128",
                "value": bson.Decimal128("10.99"),
                "atype": Decimal128Type(),
                "ptype": pl.Decimal,
            },
            {
                "type": "uuid",
                "value": uuid.uuid4(),
                "atype": BinaryType(pa.binary()),
                "ptype": pl.Binary,
            },
        ]

        # Iterate over types
        for data_type in data_type_map:
            collection.insert_one({"data_type": data_type["type"], "value": data_type["value"]})
            table = find_arrow_all(collection=collection, query={"data_type": data_type["type"]})
            assert table.shape == (1, 3)
            assert table["value"].type == data_type["atype"]
            try:
                dfpl = pl.from_arrow(table.drop("_id"))
                assert dfpl["value"].dtype == data_type["ptype"]
            except pl.ComputeError:
                assert isinstance(table["value"].type, pa.ExtensionType)
