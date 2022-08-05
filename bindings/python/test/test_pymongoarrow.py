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
from test import client_context

from pymongoarrow.api import find_arrow_all
from pymongoarrow.schema import Schema
from pymongoarrow.version import __version__


class TestPyMongoArrow(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not client_context.connected:
            raise unittest.SkipTest("cannot connect to MongoDB")
        cls.client = client_context.get_client()

    def test_version(self):
        self.assertIsNotNone(__version__)
        self.assertIsInstance(__version__, str)

    def test_capped_collection(self):
        self.client.test.drop_collection("test")
        self.client.test.create_collection("test", capped=True, size=5000)
        schema = Schema({"data": bool})
        data = [{"data": False} for _ in range(1000)]
        self.client.test.test.insert_many(data)

        # Data should have been capped.
        cursor = self.client.test.test.find({})
        total = len(list(cursor))
        self.assertLess(total, 1000)
        table = find_arrow_all(self.client.test.test, {}, schema=schema)
        self.assertEqual(table.shape, (total, 1))
