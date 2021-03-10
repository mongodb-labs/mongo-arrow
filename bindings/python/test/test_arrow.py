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
from pymongoarrow import aggregate_arrow_all, find_arrow_all, Schema

from test import client_context
from test.utils import WhiteListEventListener


class TestExplicitArrowApi(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not client_context.connected:
            raise unittest.SkipTest("cannot connect to MongoDB")
        cls.cmd_listener = WhiteListEventListener('find', 'aggregate')
        cls.getmore_listener = WhiteListEventListener('getMore')
        cls.client = client_context.get_client(
            event_listeners=[cls.getmore_listener, cls.cmd_listener])
        cls.schema = Schema({'_id': int32(), 'data': int64()})
        cls.coll = cls.client.pymongoarrow_test.get_collection(
            'test', write_concern=WriteConcern(w='majority'))

    def setUp(self):
        self.coll.drop()
        self.coll.insert_many([{'_id': 1, 'data': 10},
                               {'_id': 2, 'data': 20},
                               {'_id': 3, 'data': 30},
                               {'_id': 4}])
        self.cmd_listener.reset()
        self.getmore_listener.reset()

    def test_find_simple(self):
        expected = Table.from_pydict(
            {'_id': [1, 2, 3, 4], 'data': [10, 20, 30, None]},
            ArrowSchema([('_id', int32()), ('data', int64())]))
        table = find_arrow_all(self.coll, {}, schema=self.schema)
        self.assertEqual(table, expected)

        expected = Table.from_pydict(
            {'_id': [4, 3], 'data': [None, 30]},
            ArrowSchema([('_id', int32()), ('data', int64())]))
        table = find_arrow_all(self.coll, {'_id': {'$gt': 2}},
                               schema=self.schema,
                               sort=[('_id', DESCENDING)])
        self.assertEqual(table, expected)

        find_cmd = self.cmd_listener.results['started'][-1]
        self.assertEqual(find_cmd.command_name, 'find')
        self.assertEqual(find_cmd.command['projection'],
                         {'_id': True, 'data': True})

    def test_find_multiple_batches(self):
        orig_method = self.coll.find_raw_batches

        def mock_find_raw_batches(*args, **kwargs):
            kwargs['batch_size'] = 2
            return orig_method(*args, **kwargs)

        with mock.patch.object(
                pymongo.collection.Collection, 'find_raw_batches',
                wraps=mock_find_raw_batches):
            expected = Table.from_pydict(
                {'_id': [1, 2, 3, 4], 'data': [10, 20, 30, None]},
                ArrowSchema([('_id', int32()), ('data', int64())]))
            table = find_arrow_all(self.coll, {}, schema=self.schema)
            self.assertEqual(table, expected)
        self.assertGreater(len(self.getmore_listener.results['started']), 1)

    def test_aggregate_simple(self):
        expected = Table.from_pydict(
            {'_id': [1, 2, 3, 4], 'data': [20, 40, 60, None]},
            ArrowSchema([('_id', int32()), ('data', int64())]))
        table = aggregate_arrow_all(
            self.coll, [{'$project': {
                '_id': True, 'data': {'$multiply': [2, '$data']}}}],
            schema=self.schema)
        self.assertEqual(table, expected)

        agg_cmd = self.cmd_listener.results['started'][-1]
        self.assertEqual(agg_cmd.command_name, 'aggregate')
        self.assertEqual(agg_cmd.command['pipeline'][1]['$project'],
                         {'_id': True, 'data': True})

    def test_aggregate_multiple_batches(self):
        orig_method = self.coll.aggregate_raw_batches

        def mock_agg_raw_batches(*args, **kwargs):
            kwargs['batchSize'] = 2
            return orig_method(*args, **kwargs)

        with mock.patch.object(
                pymongo.collection.Collection, 'aggregate_raw_batches',
                wraps=mock_agg_raw_batches):
            expected = Table.from_pydict(
                {'_id': [4, 3, 2, 1], 'data': [None, 30, 20, 10]},
                ArrowSchema([('_id', int32()), ('data', int64())]))
            table = aggregate_arrow_all(
                self.coll, [{'$sort': {'_id': -1}}], schema=self.schema)
            self.assertEqual(table, expected)
        self.assertGreater(len(self.getmore_listener.results['started']), 1)
