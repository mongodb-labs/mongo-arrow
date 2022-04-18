# Write the benchmarking functions here.
# See "Writing benchmarks" in the asv docs for more information.

import collections
import math
import os
import string

import pyarrow
import pymongo
from bson import BSON
from pymongoarrow.api import Schema, find_arrow_all, find_pandas_all, write

CUR_SIZE = True if os.environ.get("BENCHMARK_SIZE") == "LARGE" else False
N_LARGE_DOCS = 1000
N_SMALL_DOCS = 100000
assert pymongo.has_c()
SMALL = False
LARGE = True
collection_names = {LARGE: "large", SMALL: "small"}
dtypes = {}
schemas = {}

arrow_tables = {}
pandas_tables = {}
large_doc_keys = None

db = pymongo.MongoClient().pymongoarrow_test
small = db[collection_names[SMALL]]
small.drop()

small.insert_many(
    [collections.OrderedDict([("x", 1), ("y", math.pi)]) for _ in range(N_SMALL_DOCS)]
)

large_doc_keys = [c * i for c in string.ascii_lowercase for i in range(1, 101)]
schemas[SMALL] = Schema({"x": pyarrow.int64(), "y": pyarrow.float64()})
schemas[LARGE] = Schema({k: pyarrow.float64() for k in large_doc_keys})
large = db[collection_names[LARGE]]
large.drop()
# 2600 keys: 'a', 'aa', 'aaa', .., 'zz..z'
large_doc = collections.OrderedDict([(k, math.pi) for k in large_doc_keys])
print(
    "%d large docs, %dk each with %d keys"
    % (N_LARGE_DOCS, len(BSON.encode(large_doc)) // 1024, len(large_doc_keys))
)

large.insert_many([large_doc.copy() for _ in range(N_LARGE_DOCS)])

arrow_tables[SMALL] = find_arrow_all(db[collection_names[SMALL]], {}, schema=schemas[SMALL])
arrow_tables[LARGE] = find_arrow_all(db[collection_names[LARGE]], {}, schema=schemas[LARGE])
pandas_tables[SMALL] = find_pandas_all(db[collection_names[SMALL]], {}, schema=schemas[SMALL])
pandas_tables[LARGE] = find_pandas_all(db[collection_names[LARGE]], {}, schema=schemas[LARGE])


class ProfileInsert:
    """
    An example benchmark that times the performance of various kinds
    of iterating over dictionaries in Python.
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
