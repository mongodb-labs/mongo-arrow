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

import tempfile
import unittest
import unittest.mock as mock
from datetime import date, datetime
from test import client_context
from test.utils import AllowListEventListener, NullsTestMixin

import pyarrow as pa
import pymongo
from bson import Binary, Code, CodecOptions, Decimal128, ObjectId
from pyarrow import (
    DataType,
    FixedSizeBinaryType,
    Table,
    bool_,
    csv,
    date32,
    date64,
    decimal256,
    field,
    int32,
    int64,
    large_list,
    large_string,
    list_,
    string,
    struct,
    timestamp,
)
from pyarrow import schema as ArrowSchema
from pyarrow.parquet import read_table, write_table
from pymongo import DESCENDING, MongoClient, WriteConcern
from pymongo.collection import Collection
from pytz import timezone

from pymongoarrow.api import Schema, aggregate_arrow_all, find_arrow_all, write
from pymongoarrow.errors import ArrowWriteError
from pymongoarrow.monkey import patch_all
from pymongoarrow.types import (
    _TYPE_NORMALIZER_FACTORY,
    BinaryType,
    CodeType,
    Decimal128Type,
    ObjectIdType,
)


class ArrowApiTestMixin:
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
            [
                {"_id": 1, "data": 10},
                {"_id": 2, "data": 20},
                {"_id": 3, "data": 30},
                {"_id": 4},
            ]
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
            {"_id": [4, 3], "data": [None, 30]},
            ArrowSchema([("_id", int32()), ("data", int64())]),
        )
        table = self.run_find({"_id": {"$gt": 2}}, schema=self.schema, sort=[("_id", DESCENDING)])
        self.assertEqual(table, expected)

        find_cmd = self.cmd_listener.results["started"][-1]
        self.assertEqual(find_cmd.command_name, "find")
        self.assertEqual(find_cmd.command["projection"], {"_id": True, "data": True})

    def test_find_with_projection(self):
        expected = Table.from_pydict(
            {"_id": [4, 3], "data": [None, 60]},
            ArrowSchema([("_id", int32()), ("data", int64())]),
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
            pymongo.collection.Collection,
            "find_raw_batches",
            wraps=mock_find_raw_batches,
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
            pymongo.collection.Collection,
            "aggregate_raw_batches",
            wraps=mock_agg_raw_batches,
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
            {
                "_id": [i for i in range(10001)] * 2,
                "data": [i * 2 for i in range(10001)] * 2,
            },
            ArrowSchema(schema),
        )
        with self.assertRaises(ArrowWriteError) as awe:
            self.round_trip(data, Schema(schema))
        self.assertEqual(
            10001,
            awe.exception.details["writeErrors"][0]["index"],
            awe.exception.details["nInserted"],
        )
        self.assertEqual(
            awe.exception.details.keys(),
            {"nInserted", "writeConcernErrors", "writeErrors"},
        )

    def test_pymongo_error(self):
        schema = {"_id": int32(), "data": int64()}
        data = Table.from_pydict(
            {
                "_id": [i for i in range(10001)] * 2,
                "data": [i * 2 for i in range(10001)] * 2,
            },
            ArrowSchema(schema),
        )

        with self.assertRaises(ArrowWriteError) as exc:
            write(
                MongoClient(
                    host="somedomainthatdoesntexist.org",
                    port=123456789,
                    serverSelectionTimeoutMS=10,
                ).pymongoarrow_test.get_collection(
                    "test", write_concern=WriteConcern(w="majority")
                ),
                data,
            )
        self.assertEqual(
            exc.exception.details.keys(),
            {"nInserted", "writeConcernErrors", "writeErrors"},
        )

    def _create_data(self):
        schema = {k.__name__: v(True) for k, v in _TYPE_NORMALIZER_FACTORY.items()}
        schema["Binary"] = BinaryType(10)
        schema["ObjectId"] = ObjectIdType()
        schema["Decimal128"] = Decimal128Type()
        schema["Code"] = CodeType()
        data = Table.from_pydict(
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
            ArrowSchema(schema),
        )
        return schema, data

    def test_write_schema_validation(self):
        schema, data = self._create_data()
        self.round_trip(data, Schema(schema))

        schema = {"_id": int32(), "data": decimal256(2)}
        data = Table.from_pydict(
            {"_id": [i for i in range(2)], "data": [i for i in range(2)]},
            ArrowSchema(schema),
        )
        with self.assertRaises(ValueError):
            self.round_trip(data, Schema(schema))

    def test_date_types(self):
        schema, data = self._create_data()
        self.round_trip(data, Schema(schema))

        schema = {"_id": int32(), "date32": date32(), "date64": date64()}
        data = Table.from_pydict(
            {
                "_id": [i for i in range(2)],
                "date32": [date(2012, 1, 1) for _ in range(2)],
                "date64": [datetime(2012, 1, 1) for _ in range(2)],
            },
            ArrowSchema(schema),
        )
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

    def _create_nested_data(self, nested_elem=None):
        schema = {k.__name__: v(0) for k, v in _TYPE_NORMALIZER_FACTORY.items()}
        if nested_elem:
            schem_ent, nested_elem = nested_elem
            schema["list"] = list_(schem_ent)

        # PyArrow does not support from_pydict with nested extension types.
        schema["nested"] = struct(
            [field(a, b) for (a, b) in list(schema.items()) if not isinstance(b, pa.ExtensionType)]
        )
        raw_data = {
            "str": [None] + [str(i) for i in range(2)],
            "bool": [True for _ in range(3)],
            "float": [0.1 for _ in range(3)],
            "Int64": [i for i in range(3)],
            "int": [i for i in range(3)],
            "datetime": [datetime(1970 + i, 1, 1) for i in range(3)],
            "Binary": [Binary(bytes(i), 10) for i in range(3)],
            "ObjectId": [ObjectId().binary for i in range(3)],
            "Decimal128": [Decimal128(str(i)).bid for i in range(3)],
            "Code": [str(i) for i in range(3)],
            "date32": [date(2012, 1, 1) for i in range(3)],
            "date64": [date(2012, 1, 1) for i in range(3)],
        }

        def inner(i):
            inner_dict = dict(
                str=str(i),
                bool=bool(i),
                float=i + 0.1,
                Int64=i,
                int=i,
                datetime=datetime(1970 + i, 1, 1),
                list=[nested_elem],
                Binary=Binary(bytes(i), 10),
                ObjectId=ObjectId().binary,
                Code=str(i),
                date32=date(2012, 1, 1),
                date64=date(2014, 1, 1),
            )
            if nested_elem:
                inner_dict["list"] = [nested_elem]
            return inner_dict

        if nested_elem:
            raw_data["list"] = [[nested_elem] for _ in range(3)]
        raw_data["nested"] = [inner(i) for i in range(3)]
        return schema, Table.from_pydict(raw_data, ArrowSchema(schema))

    def test_parquet(self):
        schema, data = self._create_nested_data()
        with tempfile.NamedTemporaryFile(suffix=".parquet") as f:
            f.close()
            write_table(data, f.name)
            data = read_table(f.name)
            self.round_trip(data, Schema(schema))

    def test_csv(self):
        # Arrow does not support struct data in csvs
        #  https://arrow.apache.org/docs/python/csv.html#reading-and-writing-csv-files
        _, data = self._create_data()
        for i in reversed(range(len(data.columns))):
            if hasattr(data.columns[i].type, "extension_name"):
                data = data.remove_column(i)

        with tempfile.NamedTemporaryFile(suffix=".csv") as f:
            f.close()
            csv.write_csv(data, f.name)
            out = csv.read_csv(f.name)
            for name in data.column_names:
                val = out[name].cast(data[name].type)
                self.assertEqual(data[name], val)

    def test_arrays_sublist(self):
        schema, data = self._create_nested_data((list_(int32()), list(range(3))))
        self.round_trip(data, Schema(schema))

    def test_arrays_subdoc(self):
        schema, data = self._create_nested_data((struct([field("a", int32())]), {"a": 32}))
        self.round_trip(data, Schema(schema))

    def test_string_bool(self):
        data = Table.from_pydict(
            {
                "string": [str(i) for i in range(2)],
                "bool": [True for _ in range(2)],
            },
            ArrowSchema(
                {
                    "string": string(),
                    "bool": bool_(),
                }
            ),
        )
        self.round_trip(
            data,
            Schema(
                {
                    "string": str,
                    "bool": bool,
                }
            ),
        )

    def test_auto_schema_nested(self):
        # Create table with random data of various types.
        _, data = self._create_nested_data()

        self.coll.drop()
        res = write(self.coll, data)
        self.assertEqual(len(data), res.raw_result["insertedCount"])
        for func in [find_arrow_all, aggregate_arrow_all]:
            out = func(self.coll, {} if func == find_arrow_all else []).drop(["_id"])
            for name in out.column_names:
                self.assertEqual(data[name], out[name].cast(data[name].type))

    def test_auto_schema(self):
        _, data = self._create_data()
        self.coll.drop()
        res = write(self.coll, data)
        self.assertEqual(len(data), res.raw_result["insertedCount"])
        for func in [find_arrow_all, aggregate_arrow_all]:
            out = func(self.coll, {} if func == find_arrow_all else []).drop(["_id"])
            for name in out.column_names:
                self.assertEqual(data[name], out[name].cast(data[name].type))

    def _test_auto_schema_list(self, docs, expected):
        self.coll.delete_many({})
        self.coll.insert_many(docs)
        actual = find_arrow_all(self.coll, {}, projection={"_id": 0})
        self.assertEqual(actual.schema, expected.schema)
        self.assertEqual(actual, expected)

    def test_auto_schema_first_list_null(self):
        docs = [
            {"a": None},
            {"a": ["str"]},
            {"a": []},
        ]
        expected = pa.Table.from_pylist(docs)
        self._test_auto_schema_list(docs, expected)

    def test_auto_schema_first_list_empty(self):
        docs = [
            {"a": []},
            {"a": ["str"]},
            {"a": []},
        ]
        expected = pa.Table.from_pylist(
            [
                {"a": None},  # TODO: We incorrectly set the first empty list to null.
                {"a": ["str"]},
                {"a": []},
            ]
        )
        self._test_auto_schema_list(docs, expected)

    def test_auto_schema_first_list_element_null(self):
        docs = [
            {"a": None},
            {"a": [None, None, "str"]},  # Inferred schema should use the first non-null element.
            {"a": []},
        ]
        expected = pa.Table.from_pylist(docs)
        self._test_auto_schema_list(docs, expected)

    @unittest.expectedFailure  # TODO: Our inferred value for the first a.b field differs from pyarrow's.
    def test_auto_schema_first_embedded_list_null(self):
        docs = [
            {"a": {"b": None}},
            {"a": {"b": ["str"]}},
            {"a": {"b": []}},
        ]
        expected = pa.Table.from_pylist(docs)
        self._test_auto_schema_list(docs, expected)

    @unittest.expectedFailure  # TODO: Our inferred value for the first a.b field differs from pyarrow's.
    def test_auto_schema_first_embedded_doc_null(self):
        docs = [
            {"a": {"b": None}},
            {"a": {"b": "str"}},
            {"a": {"b": None}},
        ]
        expected = pa.Table.from_pylist(docs)
        self._test_auto_schema_list(docs, expected)

    def test_auto_schema_heterogeneous(self):
        vals = [1, "2", True, 4]
        data = [{"a": v} for v in vals]

        self.coll.drop()
        self.coll.insert_many(data)
        for func in [find_arrow_all, aggregate_arrow_all]:
            out = func(self.coll, {} if func == find_arrow_all else []).drop(["_id"])
            self.assertEqual(out["a"].to_pylist(), [1, None, 1, 4])

    def test_auto_schema_tz(self):
        # Create table with random data of various types.
        data = Table.from_pydict(
            {
                "bool": [True for _ in range(3)],
                "dt": [datetime(1970 + i, 1, 1, tzinfo=timezone("US/Eastern")) for i in range(3)],
                "string": [None] + [str(i) for i in range(2)],
            },
            ArrowSchema(
                {
                    "bool": bool_(),
                    "dt": timestamp("ms", "US/Eastern"),
                    "string": string(),
                }
            ),
        )

        self.coll.drop()
        codec_options = CodecOptions(tzinfo=timezone("US/Eastern"), tz_aware=True)
        res = write(self.coll.with_options(codec_options=codec_options), data)
        self.assertEqual(len(data), res.raw_result["insertedCount"])
        for func in [find_arrow_all, aggregate_arrow_all]:
            out = func(
                self.coll.with_options(codec_options=codec_options),
                {} if func == find_arrow_all else [],
            ).drop(["_id"])
            self.assertEqual(out["dt"].type.tz, "US/Eastern")
            self.assertEqual(data, out)

    def test_auto_schema_tz_no_override(self):
        # Create table with random data of various types.
        # The tz of the original data is lost and replaced by the
        # tz info of the collection.
        tz = timezone("US/Pacific")
        data = Table.from_pydict(
            {
                "bool": [True for _ in range(3)],
                "dt": [datetime(1970 + i, 1, 1, tzinfo=tz) for i in range(3)],
                "string": [None] + [str(i) for i in range(2)],
            },
            ArrowSchema(
                {
                    "bool": bool_(),
                    "dt": timestamp("ms", tz=tz),
                    "string": string(),
                }
            ),
        )

        self.coll.drop()
        codec_options = CodecOptions(tzinfo=timezone("US/Eastern"), tz_aware=True)
        res = write(self.coll.with_options(codec_options=codec_options), data)
        self.assertEqual(len(data), res.raw_result["insertedCount"])
        for func in [find_arrow_all, aggregate_arrow_all]:
            out = func(
                self.coll.with_options(codec_options=codec_options),
                {} if func == find_arrow_all else [],
            ).drop(["_id"])
            self.assertEqual(out["dt"].type.tz, "US/Eastern")
            self.assertEqual(data["bool"], out["bool"])
            self.assertEqual(data["string"], out["string"])

    def test_empty_nested_objects(self):
        fields = [field("a", int32()), field("b", bool_())]
        schema = dict(top=struct(fields))
        raw_data = dict(top=[{}, {}, {}])
        data = Table.from_pydict(raw_data, ArrowSchema(schema))
        self.round_trip(data, Schema(schema))

    def test_malformed_embedded_documents(self):
        schema = Schema({"data": struct([field("a", int32()), field("b", bool_())])})
        data = [
            dict(data=dict(a=1, b=True)),
            dict(data=dict(a=1, b=True, c="bar")),
            dict(data=dict(a=1)),
            dict(data=dict(a="str", b=False)),
        ]
        self.coll.drop()
        self.coll.insert_many(data)
        res = find_arrow_all(self.coll, {}, schema=schema)["data"].to_pylist()
        self.assertEqual(
            res,
            [
                dict(a=1, b=True),
                dict(a=1, b=True),
                dict(a=1, b=None),
                dict(a=None, b=False),
            ],
        )

    def test_mixed_subtype(self):
        schema = Schema({"data": BinaryType(10)})
        coll = self.client.pymongoarrow_test.get_collection(
            "test", write_concern=WriteConcern(w="majority")
        )

        coll.drop()
        coll.insert_many([{"data": Binary(b"1", 10)}, {"data": Binary(b"2", 20)}])
        res = find_arrow_all(coll, {}, schema=schema)
        self.assertEqual(res["data"].to_pylist(), [Binary(b"1", 10), None])

    def _test_mixed_types_int(self, inttype):
        docs = [
            {"a": 1},
            {"a": 2.9},  # float should be truncated.
            {"a": True},  # True should be 1.
            {"a": False},  # False should be 0.
            {"a": float("nan")},  # Should be null.
            {"a": None},  # Should be null.
            {},  # Should be null.
            {"a": "string"},  # Should be null.
        ]
        self.coll.delete_many({})
        self.coll.insert_many(docs)
        table = find_arrow_all(self.coll, {}, projection={"_id": 0}, schema=Schema({"a": inttype}))
        expected = Table.from_pylist(
            [
                {"a": 1},
                {"a": 2},
                {"a": 1},
                {"a": 0},
                {"a": None},
                {"a": None},
                {},
                {"a": None},
            ],
            schema=ArrowSchema([field("a", inttype)]),
        )
        self.assertEqual(table, expected)

    def test_mixed_types_int32(self):
        self._test_mixed_types_int(int32())
        # Value too large to fit in int32 should cause an overflow error.
        self.coll.delete_many({})
        self.coll.insert_one({"a": 2 << 34})
        with self.assertRaises(OverflowError):
            find_arrow_all(self.coll, {}, projection={"_id": 0}, schema=Schema({"a": int32()}))
        # Test double overflowing int32
        self.coll.delete_many({})
        self.coll.insert_one({"a": float(2 << 34)})
        with self.assertRaises(OverflowError):
            find_arrow_all(self.coll, {}, projection={"_id": 0}, schema=Schema({"a": int32()}))

    def test_mixed_types_int64(self):
        self._test_mixed_types_int(int64())
        # Test double overflowing int64
        self.coll.delete_many({})
        self.coll.insert_one({"a": float(2 << 65)})
        with self.assertRaises(OverflowError):
            find_arrow_all(self.coll, {}, projection={"_id": 0}, schema=Schema({"a": int32()}))

    def test_nested_contradicting_unused_schema(self):
        data = [{"obj": {"a": 1, "b": 1000000000}}, {"obj": {"a": 2, "b": 1.0e50}}]
        schema = Schema({"obj": {"a": int32()}})

        self.coll.drop()
        self.coll.insert_many(data)
        for func in [find_arrow_all, aggregate_arrow_all]:
            out = func(self.coll, {} if func == find_arrow_all else [], schema=schema)
            self.assertEqual(out["obj"].to_pylist(), [{"a": 1}, {"a": 2}])

    def test_nested_bson_extension_types(self):
        data = {
            "obj": {
                "obj_id": ObjectId(),
                "dec_128": Decimal128("0.0005"),
                "binary": Binary(b"123"),
                "code": Code(""),
            }
        }

        self.coll.drop()
        self.coll.insert_one(data)
        out = find_arrow_all(self.coll, {})
        obj_schema_type = out.field("obj").type

        self.assertIsInstance(obj_schema_type.field("obj_id").type, FixedSizeBinaryType)
        self.assertIsInstance(obj_schema_type.field("dec_128").type, FixedSizeBinaryType)
        self.assertIsInstance(obj_schema_type.field("binary").type, DataType)
        self.assertIsInstance(obj_schema_type.field("code").type, DataType)

        new_types = [ObjectIdType(), Decimal128Type(), BinaryType(0), CodeType()]
        new_names = [f.name for f in out["obj"].type]
        new_obj = out["obj"].cast(struct(zip(new_names, new_types)))
        self.assertIsInstance(new_obj.type[0].type, ObjectIdType)
        self.assertIsInstance(new_obj.type[1].type, Decimal128Type)
        self.assertIsInstance(new_obj.type[2].type, BinaryType)
        self.assertIsInstance(new_obj.type[3].type, CodeType)

    def test_large_string_type(self):
        """Tests pyarrow._large_string() DataType"""
        data = Table.from_pydict(
            {"string": ["A", "B", "C"], "large_string": ["C", "D", "E"]},
            ArrowSchema({"string": string(), "large_string": large_string()}),
        )
        self.round_trip(data, Schema({"string": str, "large_string": large_string()}))

    def test_large_list_type(self):
        """Tests pyarrow._large_list() DataType

        1. Test large_list of large_string
            - with schema in query, one gets full roundtrip consistency
            - without schema, normal list and string will be inferred

        2. Test nested as well
        """

        schema = ArrowSchema([field("_id", int32()), field("txns", large_list(large_string()))])

        data = {
            "_id": [1, 2, 3, 4],
            "txns": [["A"], ["A", "B"], ["A", "B", "C"], ["A", "B", "C", "D"]],
        }
        table_orig = pa.Table.from_pydict(data, schema)
        self.coll.drop()
        res = write(self.coll, table_orig)
        # 1a.
        self.assertEqual(len(data["_id"]), res.raw_result["insertedCount"])
        table_schema = find_arrow_all(self.coll, {}, schema=Schema.from_arrow(schema))
        self.assertTrue(table_schema, table_orig)
        # 1b.
        table_none = find_arrow_all(self.coll, {}, schema=None)
        self.assertTrue(table_none.schema.types == [int32(), list_(string())])
        self.assertTrue(table_none.to_pydict() == data)

        # 2. Test in sublist
        schema, data = self._create_nested_data((large_list(int32()), list(range(3))))
        self.round_trip(data, Schema(schema))

    def test_binary_types(self):
        """Demonstrates that binary data is not yet supported. TODO [ARROW-214]

        Will demonstrate roundtrip behavior of Arrow DataType binary and large_binary.
        """
        for btype in [pa.binary(), pa.large_binary()]:
            with self.assertRaises(ValueError):
                self.coll.drop()
                aschema = pa.schema([("binary", btype)])
                table_in = pa.Table.from_pydict({"binary": [b"1", b"one"]}, schema=aschema)
                write(self.coll, table_in)
                table_out_none = find_arrow_all(self.coll, {}, schema=None)
                mschema = Schema.from_arrow(aschema)
                table_out_schema = find_arrow_all(self.coll, {}, schema=mschema)
                self.assertTrue(table_out_schema.schema == table_in.schema)
                self.assertTrue(table_out_none.equals(table_out_schema))


class TestArrowExplicitApi(ArrowApiTestMixin, unittest.TestCase):
    def run_find(self, *args, **kwargs):
        return find_arrow_all(self.coll, *args, **kwargs)

    def run_aggregate(self, *args, **kwargs):
        return aggregate_arrow_all(self.coll, *args, **kwargs)


class TestArrowPatchedApi(ArrowApiTestMixin, unittest.TestCase):
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
        schema = Schema({"_id": ObjectIdType(), "data": Decimal128Type()})
        expected = Table.from_pydict(
            {
                "_id": [i.binary for i in oids],
                "data": [decs[0].bid, decs[1].bid, decs[2].bid, None],
            },
            ArrowSchema([("_id", ObjectIdType()), ("data", Decimal128Type())]),
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


class TestNulls(NullsTestMixin, unittest.TestCase):
    def find_fn(self, coll, query, schema):
        return find_arrow_all(coll, query, schema=schema)

    def equal_fn(self, left, right):
        self.assertEqual(left, right)

    def table_from_dict(self, dict, schema=None):
        return pa.Table.from_pydict(dict, schema)

    def assert_in_idx(self, table, col_name):
        self.assertTrue(col_name in table.column_names)

    def na_safe(self, atype):
        return True
