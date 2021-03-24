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

from pyarrow import int32, int64
from pymongo import WriteConcern, DESCENDING
from pymongoarrow.api import aggregate_numpy_all, find_numpy_all, Schema
import numpy as np

from test import client_context
from test.utils import AllowListEventListener


class TestExplicitNumPyApi(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not client_context.connected:
            raise unittest.SkipTest("cannot connect to MongoDB")
        cls.cmd_listener = AllowListEventListener('find', 'aggregate')
        cls.getmore_listener = AllowListEventListener('getMore')
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

    def assert_numpy_equal(self, actual, expected):
        self.assertIsInstance(actual, dict)
        for field in self.schema:
            # workaround np.nan == np.nan evaluating to False
            a = np.nan_to_num(actual[field])
            e = np.nan_to_num(expected[field])
            self.assertTrue(np.all(a == e))
            self.assertEqual(actual[field].dtype, expected[field].dtype)

    def test_find_simple(self):
        expected = {
            '_id': np.array([1, 2, 3, 4], dtype=np.int32),
            # integer arrays with NaNs are given dtype float64 by NumPy
            'data': np.array([10, 20, 30, np.nan], dtype=np.float64)}

        actual = find_numpy_all(self.coll, {}, schema=self.schema)
        self.assert_numpy_equal(actual, expected)

        expected = {
            '_id': np.array([4, 3], dtype=np.int32),
            'data': np.array([np.nan, 30], dtype=np.float64)}
        actual = find_numpy_all(self.coll, {'_id': {'$gt': 2}},
                                schema=self.schema,
                                sort=[('_id', DESCENDING)])
        self.assert_numpy_equal(actual, expected)

        find_cmd = self.cmd_listener.results['started'][-1]
        self.assertEqual(find_cmd.command_name, 'find')
        self.assertEqual(find_cmd.command['projection'],
                         {'_id': True, 'data': True})

    def test_aggregate_simple(self):
        expected = {
            '_id': np.array([1, 2, 3, 4], dtype=np.int32),
            'data': np.array([20, 40, 60, np.nan], dtype=np.float64)}

        actual = aggregate_numpy_all(
            self.coll, [{'$project': {
                '_id': True, 'data': {'$multiply': [2, '$data']}}}],
            schema=self.schema)
        self.assert_numpy_equal(actual, expected)

        agg_cmd = self.cmd_listener.results['started'][-1]
        self.assertEqual(agg_cmd.command_name, 'aggregate')
        self.assertEqual(agg_cmd.command['pipeline'][1]['$project'],
                         {'_id': True, 'data': True})
