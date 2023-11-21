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
from datetime import datetime
from unittest import TestCase

from bson import Binary, Code, Decimal128, Int64, ObjectId
from pyarrow import Table, field, float64, int64, list_, struct, timestamp
from pyarrow import schema as ArrowSchema

from pymongoarrow.schema import Schema
from pymongoarrow.types import _TYPE_NORMALIZER_FACTORY


class TestSchema(TestCase):
    def test_as_py(self):
        # Some of the classes want special things in their constructors.
        instantiated_objs = {
            datetime: datetime(1, 1, 1),
            str: "hell0",
            bool: True,
        }
        # The extension types need to be provided to from_pydict as strings or binary,
        # but we also need the original object for the assertion at the end of the test.
        oid = ObjectId()
        dec = Decimal128("1.000")
        binary = Binary(bytes(10), 10)
        code = Code("hi", None)
        lookup = {Decimal128: dec, ObjectId: oid, Binary: binary, Code: code}
        instantiated_objs.update(
            {Decimal128: dec.bid, ObjectId: oid.binary, Binary: binary, Code: code}
        )

        for k, v in _TYPE_NORMALIZER_FACTORY.items():
            # Make an array of 4 elements with either the instantiated object or 1.
            column = [instantiated_objs.get(k, 1)] * 4
            val = True if k is not Binary else 10
            t = Table.from_pydict(
                {"value": column},
                ArrowSchema([("value", v(val))]),
            )
            self.assertEqual(t.to_pylist(), [{"value": lookup.get(k, i)} for i in column])

    def test_initialization(self):
        dict_schema = Schema({"field1": int, "field2": datetime, "field3": float})
        self.assertEqual(
            dict_schema.typemap,
            {"field1": int64(), "field2": timestamp("ms"), "field3": float64()},
        )

    def test_from_py_units(self):
        schema = Schema({"field1": int, "field2": datetime, "field3": float})
        self.assertEqual(
            schema.typemap,
            {"field1": int64(), "field2": timestamp("ms"), "field3": float64()},
        )

    def test_from_bson_units(self):
        schema = Schema({"field1": Int64})
        self.assertEqual(schema.typemap, {"field1": int64()})

    def test_from_arrow_units(self):
        schema = Schema({"field1": int64(), "field2": timestamp("s")})
        self.assertEqual(schema.typemap, {"field1": int64(), "field2": timestamp("s")})

    def test_nested_projection(self):
        schema = Schema({"_id": int64(), "obj": {"a": int64(), "b": int64()}})
        self.assertEqual(schema._get_projection(), {"_id": True, "obj": {"a": True, "b": True}})

    def test_list_projection(self):
        schema = Schema(
            {
                "_id": int64(),
                "list": list_(struct([field("a", int64()), field("b", int64())])),
            }
        )
        self.assertEqual(schema._get_projection(), {"_id": True, "list": {"a": True, "b": True}})

    def test_list_of_list_projection(self):
        schema = Schema(
            {
                "_id": int64(),
                "list": list_(list_(struct([field("a", int64()), field("b", int64())]))),
            }
        )
        self.assertEqual(schema._get_projection(), {"_id": True, "list": {"a": True, "b": True}})
