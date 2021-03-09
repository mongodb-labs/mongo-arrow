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

from pyarrow import int32, int64, schema as ArrowSchema, Table
from pymongo import WriteConcern, DESCENDING
import pymongo
from pymongoarrow.api import find_arrow_all, Schema

from test import client_context
from test.utils import WhiteListEventListener


class TestArrow(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not client_context.connected:
            raise unittest.SkipTest("cannot connect to MongoDB")
        cls.listener = WhiteListEventListener('getMore')
        cls.client = client_context.get_client(event_listeners=[cls.listener])
        cls.schema = Schema({'_id': int32(), 'data': int64()})
        cls.coll = cls.client.pymongoarrow_test.get_collection(
            'test', write_concern=WriteConcern(w='majority'))

    def _run_test(self, doclist, query, expectation, **kwargs):
        self.coll.drop()
        self.listener.reset()
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

        expected = Table.from_pydict(
            {'_id': [4, 3], 'data': [40, 30]},
            ArrowSchema([('_id', int32()), ('data', int64())]))
        self._run_test(docs, {'_id': {'$gt': 2}}, expected,
                       sort=[('_id', DESCENDING)])

    def test_multiple_batches(self):
        docs = [{'_id': 1, 'data': 10},
                {'_id': 2, 'data': 20},
                {'_id': 3, 'data': 30},
                {'_id': 4, 'data': 40}]

        orig_method = self.coll.find_raw_batches

        def mock_find_raw_batches(*args, **kwargs):
            kwargs['batch_size'] = 2
            return orig_method(*args, **kwargs)

        with mock.patch.object(
                pymongo.collection.Collection, 'find_raw_batches',
                wraps=mock_find_raw_batches):
            expected = Table.from_pydict(
                {'_id': [1, 2, 3, 4], 'data': [10, 20, 30, 40]},
                ArrowSchema([('_id', int32()), ('data', int64())]))
            self._run_test(docs, {}, expected)
        self.assertGreater(len(self.listener.results['started']), 1)

    def test_with_nulls(self):
        docs = [{'_id': 1, 'data': 10},
                {'_id': 2},
                {'_id': 3, 'data': 30},
                {'_id': 4, 'rawdata': 40}]

        expected = Table.from_pydict(
            {'_id': [1, 2, 3, 4], 'data': [10, None, 30, None]},
            ArrowSchema([('_id', int32()), ('data', int64())]))
        self._run_test(docs, {}, expected)

    def test_with_project(self):
        docs = [{'_id': 1, 'data': 10},
                {'_id': 2, 'data': 20},
                {'_id': 3, 'data': 30},
                {'_id': 4, 'data': 40}]
        expected = Table.from_pydict(
            {'_id': [None] * 4, 'data': [10, 20, 30, 40]},
            ArrowSchema([('_id', int32()), ('data', int64())]))
        self._run_test(docs, {}, expected, projection={'_id': False})
