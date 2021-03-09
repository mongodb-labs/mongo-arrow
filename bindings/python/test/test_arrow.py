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

from pyarrow import int32, int64, schema as ArrowSchema, Table
from pymongo import WriteConcern
from pymongoarrow.api import find_arrow_all, Schema

from test import client_context


class TestArrow(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not client_context.connected:
            raise unittest.SkipTest("cannot connect to MongoDB")
        cls.client = client_context.client

    def setUp(self):
        self.schema = Schema({'_id': int32(),
                              'data': int64()})
        self.coll = self.client.pymongoarrow_test.get_collection(
            'test', write_concern=WriteConcern(w='majority'))
        self.coll.drop()

    def _run_test(self, doclist, query, expectation, **kwargs):
        self.coll.insert_many(doclist)
        table = find_arrow_all(self.coll, query, self.schema, **kwargs)
        self.assertEqual(table, expectation)

    def test_simple(self):
        docs = [{'_id': 1, 'data': 10},
                {'_id': 2, 'data': 20},
                {'_id': 3, 'data': 30},
                {'_id': 4, 'data': 40}]
        expected = Table.from_pydict(
            {'_id': [1, 2, 3, 4], 'data': [10, 20, 30, 40]},
            ArrowSchema([('_id', int32()), ('data', int64())]))

        self._run_test(docs, {}, expected)
