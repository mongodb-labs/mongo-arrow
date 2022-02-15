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

from bson import Int64
from pyarrow import float64, int64, timestamp
from pymongoarrow.schema import Schema


class TestSchema(TestCase):
    def test_initialization(self):
        dict_schema = Schema({"field1": int, "field2": datetime, "field3": float})
        self.assertEqual(
            dict_schema.typemap, {"field1": int64(), "field2": timestamp("ms"), "field3": float64()}
        )

    def test_from_py_units(self):
        schema = Schema({"field1": int, "field2": datetime, "field3": float})
        self.assertEqual(
            schema.typemap, {"field1": int64(), "field2": timestamp("ms"), "field3": float64()}
        )

    def test_from_bson_units(self):
        schema = Schema({"field1": Int64})
        self.assertEqual(schema.typemap, {"field1": int64()})

    def test_from_arrow_units(self):
        schema = Schema({"field1": int64(), "field2": timestamp("s")})
        self.assertEqual(schema.typemap, {"field1": int64(), "field2": timestamp("s")})
