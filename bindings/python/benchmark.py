import argparse
import collections
import math
import string
import sys
import timeit
from functools import partial

import numpy as np
import pandas as pd
import pyarrow
import pymongo
from bson import BSON, Int64, ObjectId
from pymongoarrow.api import (
    Schema,
    find_arrow_all,
    find_numpy_all,
    find_pandas_all,
    write,
)

assert pymongo.has_c()

# Use large document in tests? If SMALL, no, if LARGE, then yes.
SMALL = False
LARGE = True
db = None
raw_bson = None
large_doc_keys = None
collection_names = {LARGE: "large", SMALL: "small"}
dtypes = {}
schemas = {}
raw_bsons = {}

arrow_tables = {}
pandas_tables = {}
numpy_arrays = {}


def _setup():
    global db
    global raw_bson
    global large_doc_keys

    db = pymongo.MongoClient().pymongoarrow_test
    small = db[collection_names[SMALL]]
    small.drop()

    if SMALL in sizes:
        print(
            "%d small docs, %d bytes each with 3 keys"
            % (N_SMALL_DOCS, len(BSON.encode({"_id": ObjectId(), "x": 1, "y": math.pi})))
        )

        small.insert_many(
            [collections.OrderedDict([("x", 1), ("y", math.pi)]) for _ in range(N_SMALL_DOCS)]
        )

        dtypes[SMALL] = np.dtype([("x", np.int64), ("y", np.float64)])
        schemas[SMALL] = Schema({"x": pyarrow.int64(), "y": pyarrow.float64()})

        # Ignore for now that the first batch defaults to 101 documents.
        raw_bson_docs_small = [{"x": 1, "y": math.pi} for _ in range(N_SMALL_DOCS)]
        raw_bson_small = BSON.encode(
            {
                "ok": 1,
                "cursor": {
                    "id": Int64(1234),
                    "ns": "db.collection",
                    "firstBatch": raw_bson_docs_small,
                },
            }
        )

    if LARGE in sizes:
        large = db[collection_names[LARGE]]
        large.drop()
        # 2600 keys: 'a', 'aa', 'aaa', .., 'zz..z'
        large_doc_keys = [c * i for c in string.ascii_lowercase for i in range(1, 101)]
        large_doc = collections.OrderedDict([(k, math.pi) for k in large_doc_keys])
        print(
            "%d large docs, %dk each with %d keys"
            % (N_LARGE_DOCS, len(BSON.encode(large_doc)) // 1024, len(large_doc_keys))
        )

        large.insert_many([large_doc.copy() for _ in range(N_LARGE_DOCS)])

        dtypes[LARGE] = np.dtype([(k, np.float64) for k in large_doc_keys])
        schemas[LARGE] = Schema({k: pyarrow.float64() for k in large_doc_keys})

        raw_bson_docs_large = [large_doc.copy() for _ in range(N_LARGE_DOCS)]
        raw_bson_large = BSON.encode(
            {
                "ok": 1,
                "cursor": {
                    "id": Int64(1234),
                    "ns": "db.collection",
                    "firstBatch": raw_bson_docs_large,
                },
            }
        )

    if SMALL in sizes:
        raw_bsons[SMALL] = raw_bson_small
        arrow_tables[SMALL] = find_arrow_all(db[collection_names[SMALL]], {}, schema=schemas[SMALL])
        assert arrow_tables[SMALL].shape == (100000, 2)
        pandas_tables[SMALL] = find_pandas_all(
            db[collection_names[SMALL]], {}, schema=schemas[SMALL]
        )
        numpy_arrays[SMALL] = find_numpy_all(db[collection_names[SMALL]], {}, schema=schemas[SMALL])

    if LARGE in sizes:
        raw_bsons[LARGE] = raw_bson_large
        arrow_tables[LARGE] = find_arrow_all(db[collection_names[LARGE]], {}, schema=schemas[LARGE])
        assert arrow_tables[LARGE].shape == (1000, 2600)
        pandas_tables[LARGE] = find_pandas_all(
            db[collection_names[LARGE]], {}, schema=schemas[LARGE]
        )
        numpy_arrays[LARGE] = find_numpy_all(db[collection_names[LARGE]], {}, schema=schemas[LARGE])


def _teardown():
    db.collection.drop()


bench_fns = collections.OrderedDict()


def bench(name):
    def assign_name(fn):
        assert fn not in bench_fns.values()
        bench_fns[name] = fn
        return fn

    return assign_name


@bench("conventional-to-ndarray")
def conventional_ndarray(use_large):
    collection = db[collection_names[use_large]]
    cursor = collection.find()
    dtype = dtypes[use_large]

    if use_large:
        np.array([tuple(doc[k] for k in large_doc_keys) for doc in cursor], dtype=dtype)
    else:
        np.array([(doc["x"], doc["y"]) for doc in cursor], dtype=dtype)


# Note: this is called "to-numpy" and not "to-ndarray" because find_numpy_all
# does not produce an ndarray.
@bench("pymongoarrow-to-numpy")
def to_numpy(use_large):
    c = db[collection_names[use_large]]
    schema = schemas[use_large]
    find_numpy_all(c, {}, schema=schema)


@bench("conventional-to-pandas")
def conventional_pandas(use_large):
    collection = db[collection_names[use_large]]
    _ = dtypes[use_large]
    cursor = collection.find(projection={"_id": 0})
    _ = pd.DataFrame(list(cursor))


@bench("pymongoarrow-to-pandas")
def to_pandas(use_large):
    c = db[collection_names[use_large]]
    schema = schemas[use_large]
    find_pandas_all(c, {}, schema=schema)


@bench("pymongoarrow-to-arrow")
def to_arrow(use_large):
    c = db[collection_names[use_large]]
    schema = schemas[use_large]
    find_arrow_all(c, {}, schema=schema)


@bench("insert_arrow")
def insert_arrow(use_large):
    write(db[collection_names[use_large]], arrow_tables[use_large])


@bench("insert_conventional")
def insert_conventional(use_large):
    tab = arrow_tables[use_large].to_pylist()
    db[collection_names[use_large]].insert_many(tab)


@bench("insert_pandas")
def insert_pandas(use_large):
    write(db[collection_names[use_large]], pandas_tables[use_large])


@bench("insert_numpy")
def insert_numpy(use_large):
    write(db[collection_names[use_large]], numpy_arrays[use_large])


parser = argparse.ArgumentParser(
    formatter_class=argparse.RawTextHelpFormatter,
    epilog="""
Available benchmark functions:
   %s
"""
    % ("\n   ".join(bench_fns.keys()),),
)
parser.add_argument("--large", action="store_true", help="only test with large documents")
parser.add_argument("--small", action="store_true", help="only test with small documents")
parser.add_argument("--test", action="store_true", help="quick test of benchmark.py")
parser.add_argument("funcs", nargs="*", default=bench_fns.keys())
options = parser.parse_args()

if options.test:
    N_LARGE_DOCS = 2
    N_SMALL_DOCS = 2
    N_TRIALS = 1
else:
    N_LARGE_DOCS = 1000
    N_SMALL_DOCS = 100000
    N_TRIALS = 5

# Run tests with both small and large documents.
sizes = [SMALL, LARGE]
if options.large and not options.small:
    sizes.remove(SMALL)
if options.small and not options.large:
    sizes.remove(LARGE)

for name in options.funcs:
    if name not in bench_fns:
        sys.stderr.write('Unknown function "%s"\n' % name)
        sys.stderr.write("Available functions:\n%s\n" % ("\n".join(bench_fns)))
        sys.exit(1)

_setup()

print()
print("%25s: %7s %7s" % ("BENCH", "SMALL", "LARGE"))

for name, fn in bench_fns.items():
    if name in options.funcs:
        sys.stdout.write("%25s: " % name)
        sys.stdout.flush()

        # Test with small and large documents.
        for size in (SMALL, LARGE):
            if size not in sizes:
                sys.stdout.write("%7s" % "-")
            else:
                timer = timeit.Timer(partial(fn, size))
                duration = min(timer.repeat(3, N_TRIALS)) / float(N_TRIALS)
                sys.stdout.write("%7.2f " % duration)
            sys.stdout.flush()

        sys.stdout.write("\n")

_teardown()
