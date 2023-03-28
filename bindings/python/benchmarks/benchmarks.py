# Copyright 2022-present MongoDB, Inc.
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

import collections
import math
import os

import numpy as np
import pandas as pd
import pyarrow
import pymongo
from bson import BSON
from pymongoarrow.api import (
    Schema,
    find_arrow_all,
    find_numpy_all,
    find_pandas_all,
    write,
)

CUR_SIZE = True if os.environ.get("BENCHMARK_SIZE") == "LARGE" else False
N_LARGE_DOCS = 1000
N_SMALL_DOCS = 100000
assert pymongo.has_c()
SMALL = 1
LARGE = 2
LIST = 3
collection_names = {LARGE: "large", SMALL: "small", LIST: "list"}
dtypes = {}
schemas = {}

arrow_tables = {}
pandas_tables = {}
numpy_arrays = {}

large_doc_keys = None

db = pymongo.MongoClient().pymongoarrow_test
small = db[collection_names[SMALL]]
small.drop()

small.insert_many(
    [collections.OrderedDict([("x", 1), ("y", math.pi)]) for _ in range(N_SMALL_DOCS)]
)
base_small = collections.OrderedDict(
    [("x", 1), ("y", math.pi), ("list", [math.pi for _ in range(64)])]
)
small.insert_many([base_small.copy() for _ in range(N_SMALL_DOCS)])

large_doc_keys = [f"a{i}" for i in range(2600)]
schemas[SMALL] = Schema({"x": pyarrow.int64(), "y": pyarrow.float64()})
schemas[LIST + SMALL] = Schema({"list": pyarrow.list_(pyarrow.float64())})
dtypes[SMALL] = np.dtype([("x", np.int64), ("y", np.float64)])
dtypes[LARGE] = np.dtype([(k, np.float64) for k in large_doc_keys])
schemas[LARGE] = Schema({k: pyarrow.float64() for k in large_doc_keys})
schemas[LIST + LARGE] = Schema(
    {k + "_list": pyarrow.list_(pyarrow.float64()) for k in large_doc_keys}
)
large = db[collection_names[LARGE]]
large.drop()
large_doc = {k: math.pi for k in large_doc_keys}
large_doc.update({k + "_list": [math.pi for _ in range(64)] for k in large_doc_keys})
print(
    "%d large docs, %dk each with %d keys"
    % (N_LARGE_DOCS, len(BSON.encode(large_doc)) // 1024, len(large_doc_keys))
)

large.insert_many([large_doc.copy() for _ in range(N_LARGE_DOCS)])

arrow_tables[SMALL] = find_arrow_all(db[collection_names[SMALL]], {}, schema=schemas[SMALL])
arrow_tables[LARGE] = find_arrow_all(db[collection_names[LARGE]], {}, schema=schemas[LARGE])
pandas_tables[SMALL] = find_pandas_all(db[collection_names[SMALL]], {}, schema=schemas[SMALL])
pandas_tables[LARGE] = find_pandas_all(db[collection_names[LARGE]], {}, schema=schemas[LARGE])
numpy_arrays[SMALL] = find_numpy_all(db[collection_names[SMALL]], {}, schema=schemas[SMALL])
numpy_arrays[LARGE] = find_numpy_all(db[collection_names[LARGE]], {}, schema=schemas[LARGE])


class ProfileInsert:
    """
    A benchmark that times the performance of various kinds
    of inserting tabular data.
    """

    def setup(self):
        db[collection_names[CUR_SIZE]].drop()

    def time_insert_arrow(self):
        write(db[collection_names[CUR_SIZE]], arrow_tables[CUR_SIZE])

    def time_insert_conventional(self):
        tab = arrow_tables[CUR_SIZE].to_pylist()
        db[collection_names[CUR_SIZE]].insert_many(tab)

    def time_insert_pandas(self):
        write(db[collection_names[CUR_SIZE]], pandas_tables[CUR_SIZE])

    def time_insert_numpy(self):
        write(db[collection_names[CUR_SIZE]], numpy_arrays[CUR_SIZE])


class ProfileRead:
    """
    A benchmark that times the performance of various kinds
    of reading MongoDB data.
    """

    # We need this because the naive methods don't always convert nested objects.
    @staticmethod
    def exercise_table(table):
        [
            [[n for n in i.values] if isinstance(i, pyarrow.ListScalar) else i for i in column]
            for column in table.columns
        ]

    def setup(self):
        db[collection_names[CUR_SIZE]].drop()

    def time_conventional_ndarray(self):
        collection = db[collection_names[CUR_SIZE]]
        cursor = collection.find()
        dtype = dtypes[CUR_SIZE]

        if CUR_SIZE == LARGE:
            np.array([tuple(doc[k] for k in large_doc_keys) for doc in cursor], dtype=dtype)
        else:
            np.array([(doc["x"], doc["y"]) for doc in cursor], dtype=dtype)

    def time_to_numpy(self):
        c = db[collection_names[CUR_SIZE]]
        schema = schemas[CUR_SIZE]
        find_numpy_all(c, {}, schema=schema)

    def time_conventional_pandas(self):
        collection = db[collection_names[CUR_SIZE]]
        _ = dtypes[CUR_SIZE]
        cursor = collection.find(projection={"_id": 0})
        _ = pd.DataFrame(list(cursor))

    def time_to_pandas(self):
        c = db[collection_names[CUR_SIZE]]
        schema = schemas[CUR_SIZE]
        find_pandas_all(c, {}, schema=schema)

    def time_to_arrow(self):
        c = db[collection_names[CUR_SIZE]]
        schema = schemas[CUR_SIZE]
        find_arrow_all(c, {}, schema=schema)

    def time_to_arrow_arrays(self):
        c = db[collection_names[CUR_SIZE]]
        schema = schemas[LIST + CUR_SIZE]
        table = find_arrow_all(c, {}, schema=schema, projection={"_id": 0})
        self.exercise_table(table)

    def time_to_conventional_arrays(self):
        c = db[collection_names[CUR_SIZE]]
        f = list(c.find({}, projection={"_id": 0}))
        table = pyarrow.Table.from_pylist(f)
        self.exercise_table(table)
