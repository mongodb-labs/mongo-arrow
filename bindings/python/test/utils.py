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
import datetime
import unittest
from collections import defaultdict
from test import client_context

import numpy as np
import pyarrow as pa
from bson import Decimal128, ObjectId
from pandas import isna
from pyarrow import bool_, float64, int64, string, timestamp
from pymongo import WriteConcern, monitoring

from pymongoarrow.api import write
from pymongoarrow.schema import Schema
from pymongoarrow.types import (
    _TYPE_NORMALIZER_FACTORY,
    Decimal128Type,
    ObjectIdType,
    _in_type_map,
)


class EventListener(monitoring.CommandListener):
    def __init__(self):
        self.results = defaultdict(list)

    def started(self, event):
        self.results["started"].append(event)

    def succeeded(self, event):
        self.results["succeeded"].append(event)

    def failed(self, event):
        self.results["failed"].append(event)

    def started_command_names(self):
        """Return list of command names started."""
        return [event.command_name for event in self.results["started"]]

    def reset(self):
        """Reset the state of this listener."""
        self.results.clear()


class AllowListEventListener(EventListener):
    def __init__(self, *commands):
        self.commands = set(commands)
        super().__init__()

    def started(self, event):
        if event.command_name in self.commands:
            super().started(event)

    def succeeded(self, event):
        if event.command_name in self.commands:
            super().succeeded(event)

    def failed(self, event):
        if event.command_name in self.commands:
            super().failed(event)


class NullsTestMixin:
    def find_fn(self, coll, query, schema):
        raise NotImplementedError

    def equal_fn(self, left, right):
        raise NotImplementedError

    def table_from_dict(self, dict, schema=None):
        raise NotImplementedError

    def assert_in_idx(self, table, col_name):
        raise NotImplementedError

    def na_safe(self, atype):
        raise NotImplementedError

    # Map Python types to constructors
    pytype_cons_map = {
        str: str,
        int: int,
        float: float,
        datetime.datetime: lambda x: datetime.datetime(x + 1970, 1, 1),
        ObjectId: lambda _: ObjectId(),
        Decimal128: lambda x: Decimal128([x, x]),
    }

    # Map Python types to types for table we are comparing.
    pytype_tab_map = {
        str: string(),
        int: int64(),
        float: float64(),
        datetime.datetime: timestamp("ms"),
        ObjectId: ObjectIdType(),
        Decimal128: Decimal128Type(),
        bool: bool_(),
    }

    pytype_writeback_exc_map = {
        str: None,
        int: None,
        float: None,
        datetime.datetime: None,
        ObjectId: pa.lib.ArrowInvalid,
        Decimal128: pa.lib.ArrowInvalid,
        bool: None,
    }

    @classmethod
    def setUpClass(cls):
        if cls is NullsTestMixin:
            raise unittest.SkipTest("Base class")

        if not client_context.connected:
            raise unittest.SkipTest("cannot connect to MongoDB")
        cls.cmd_listener = AllowListEventListener("find", "aggregate")
        cls.getmore_listener = AllowListEventListener("getMore")
        cls.client = client_context.get_client(
            event_listeners=[cls.getmore_listener, cls.cmd_listener]
        )

        cls.oids = [ObjectId() for _ in range(4)]
        cls.coll = cls.client.pymongoarrow_test.get_collection(
            "test", write_concern=WriteConcern(w="majority")
        )

    def setUp(self):
        self.coll.drop()

        self.cmd_listener.reset()
        self.getmore_listener.reset()

    def assertType(self, obj1, arrow_type):
        if isinstance(obj1, pa.ChunkedArray):
            if "storage_type" in dir(arrow_type) and obj1.type != arrow_type:
                self.assertEqual(obj1.type, arrow_type.storage_type)
            else:
                self.assertEqual(obj1.type, arrow_type)
        else:
            if isinstance(arrow_type, list):
                self.assertTrue(obj1.dtype.name in arrow_type)
            else:
                self.assertEqual(obj1.dtype.name, arrow_type)

    def test_int_handling(self):
        # Default integral types
        int_schema = Schema({"_id": ObjectIdType(), "int64": int64()})
        int64_arr = [(i if (i % 2 == 0) else None) for i in range(len(self.oids))]
        self.coll.insert_many(
            [{"_id": self.oids[i], "int64": int64_arr[i]} for i in range(len(self.oids))]
        )

        table = self.find_fn(self.coll, {}, schema=int_schema)

        # Resulting datatype should be float64 according to the spec for numpy
        # and pandas.
        atype = self.pytype_tab_map[int]
        self.assertType(table["int64"], atype)

        # Does it contain NAs where we expect?
        self.assertTrue(np.all(np.equal(isna(int64_arr), isna(table["int64"]))))

        # Write
        self.coll.drop()
        table_write = self.table_from_dict({"int64": int64_arr})

        write(self.coll, table_write)
        res_table = self.find_fn(self.coll, {}, schema=int_schema)

        self.equal_fn(res_table["int64"], table_write["int64"])
        self.assert_in_idx(res_table, "_id")
        self.assertType(res_table["int64"], atype)

    def test_all_types(self):
        for t in self.pytype_tab_map:
            self.assertTrue(_in_type_map(_TYPE_NORMALIZER_FACTORY[t](0)))

    def test_other_handling(self):
        # Tests other types, which are treated differently in
        # arrow/pandas/numpy.
        for gen in [str, float, datetime.datetime, ObjectId, Decimal128]:
            con_type = self.pytype_tab_map[gen]  # Arrow/Pandas/Numpy
            pytype = NullsTestMixin.pytype_tab_map[gen]  # Arrow type specifically

            other_schema = Schema({"_id": ObjectIdType(), "other": pytype})
            others = [
                self.pytype_cons_map[gen](i) if (i % 2 == 0) else None
                for i in range(len(self.oids))
            ]

            self.setUp()
            self.coll.insert_many(
                [{"_id": self.oids[i], "other": others[i]} for i in range(len(self.oids))]
            )
            table = self.find_fn(self.coll, {}, schema=other_schema)

            # Resulting datatype should be str in this case

            self.assertType(table["other"], con_type)
            self.assertEqual(
                self.na_safe(con_type),
                np.all(np.equal(isna(others), isna(table["other"]))),
            )

            def writeback():
                # Write
                self.coll.drop()
                table_write_schema = Schema({"other": pytype})
                table_write_schema_arrow = (
                    pa.schema([("other", pytype)])
                    if (gen in [str, float, datetime.datetime])
                    else None
                )

                table_write = self.table_from_dict(
                    {"other": others}, schema=table_write_schema_arrow
                )

                write(self.coll, table_write)
                res_table = self.find_fn(self.coll, {}, schema=table_write_schema)
                self.equal_fn(res_table, table_write)
                self.assertType(res_table["other"], con_type)

            # Do we expect an exception to be raised?
            if self.pytype_writeback_exc_map[gen] is not None:
                expected_exc = self.pytype_writeback_exc_map[gen]
                with self.assertRaises(expected_exc):
                    writeback()
            else:
                writeback()

    def test_bool_handling(self):
        atype = self.pytype_tab_map[bool]
        bool_schema = Schema({"_id": ObjectIdType(), "bool_": bool_()})
        bools = [True if (i % 2 == 0) else None for i in range(len(self.oids))]

        self.coll.insert_many(
            [{"_id": self.oids[i], "bool_": bools[i]} for i in range(len(self.oids))]
        )

        table = self.find_fn(self.coll, {}, schema=bool_schema)

        # Resulting datatype should be object
        self.assertType(table["bool_"], atype)

        # Does it contain Nones where expected?
        self.assertTrue(np.all(np.equal(isna(bools), isna(table["bool_"]))))
