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
from collections import defaultdict
from unittest import TestCase

from bson import encode

from pymongoarrow.context import PyMongoArrowContext
from pymongoarrow.lib import process_bson_stream
from pymongoarrow.schema import Schema
from pymongoarrow.types import int32, int64


class TestBSONUtil(TestCase):
    @classmethod
    def setUpClass(cls):
        docs = [{'_id': 1, 'data': 10},
                {'_id': 2, 'data': 20},
                {'_id': 3, 'data': 30},
                {'_id': 4, 'data': 40}]

        payload = b''
        for doc in docs:
            payload += encode(doc)

        as_dict = defaultdict(list)
        for doc in docs:
            for key, value in doc.items():
                as_dict[key].append(value)

        cls.as_dict = as_dict
        cls.docs = docs
        cls.schema = Schema({'_id': int32(),
                             'data': int64()})
        cls.bson_stream = payload

    def test_simple(self):
        context = PyMongoArrowContext.from_schema(self.schema)
        process_bson_stream(self.bson_stream, context)
        table = context.finish()
        table_dict = table.to_pydict()

        for key, value in table_dict.items():
            self.assertEqual(value, self.as_dict[key])

        print(table.to_pandas())
