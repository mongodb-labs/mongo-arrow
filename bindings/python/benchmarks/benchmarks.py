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
import abc
import math
import os
from abc import ABC

import numpy as np
import pandas as pd
import polars as pl
import pyarrow as pa
import pymongo
from bson import BSON, Binary, Decimal128

from pymongoarrow.api import (
    Schema,
    find_arrow_all,
    find_numpy_all,
    find_pandas_all,
    find_polars_all,
    write,
)
from pymongoarrow.types import BinaryType, Decimal128Type

N_DOCS = int(os.environ.get("N_DOCS"))
assert pymongo.has_c()  # noqa: S101
db = pymongo.MongoClient().pymongoarrow_test

LARGE_DOC_SIZE = 20
EMBEDDED_OBJECT_SIZE = (
    20  # The number of values or key/value pairs in the embedded object (array or document).
)


# We have to use ABCs because ASV doesn't support any other way of skipping tests.
class Insert(ABC):
    """
    A benchmark that times the performance of various kinds
    of inserting tabular data.
    """

    timeout = 100000  # The setup sometimes times out.
    number = 1
    repeat = (
        1,
        10,
        30.0,
    )  # Min repeat, max repeat, time limit (will stop sampling after this)
    rounds = 1

    @abc.abstractmethod
    def setup(self):
        raise NotImplementedError

    def time_insert_arrow(self):
        write(db.benchmark, self.arrow_table)

    def time_insert_conventional(self):
        tab = self.arrow_table.to_pylist()
        db.benchmark.insert_many(tab)

    def time_insert_pandas(self):
        write(db.benchmark, self.pandas_table)

    def time_insert_numpy(self):
        write(db.benchmark, self.numpy_arrays)

    def time_insert_polars(self):
        write(db.benchmark, self.polars_table)

    def peakmem_insert_arrow(self):
        self.time_insert_arrow()

    def peakmem_insert_conventional(self):
        self.time_insert_conventional()

    def peakmem_insert_pandas(self):
        self.time_insert_pandas()

    def peakmem_insert_numpy(self):
        self.time_insert_numpy()

    def peakmem_insert_polars(self):
        self.time_insert_polars()


class Read(ABC):
    """
    A benchmark that times the performance of various kinds
    of reading MongoDB data.
    """

    timeout = 100000  # The setup sometimes times out.
    number = 3
    repeat = (
        1,
        10,
        30.0,
    )  # Min repeat, max repeat, time limit (will stop sampling after this)
    rounds = 1

    @abc.abstractmethod
    def setup(self):
        raise NotImplementedError

    # We need this because the naive methods don't always convert nested objects.
    @staticmethod  # noqa: B027
    def exercise_table(table):
        pass

    def time_conventional_ndarray(self):
        collection = db.benchmark
        cursor = collection.find(projection={"_id": 0})
        dtype = self.dtypes
        if "Large" in type(self).__name__:
            np.array(
                [tuple(doc[k] for k in self.large_doc_keys) for doc in cursor],
                dtype=dtype,
            )
        else:
            np.array([(doc["x"], doc["y"]) for doc in cursor], dtype=dtype)

    def time_to_numpy(self):
        c = db.benchmark
        find_numpy_all(c, {}, schema=self.schema, projection={"_id": 0})

    def time_conventional_pandas(self):
        collection = db.benchmark
        cursor = collection.find(projection={"_id": 0})
        _ = pd.DataFrame(list(cursor))

    def time_to_pandas(self):
        c = db.benchmark
        find_pandas_all(c, {}, schema=self.schema, projection={"_id": 0})

    def time_conventional_arrow(self):
        c = db.benchmark
        f = list(c.find({}, projection={"_id": 0}))
        table = pa.Table.from_pylist(f)
        self.exercise_table(table)

    def time_to_arrow(self):
        c = db.benchmark
        table = find_arrow_all(c, {}, schema=self.schema, projection={"_id": 0})
        self.exercise_table(table)

    def time_conventional_polars(self):
        collection = db.benchmark
        cursor = collection.find(projection={"_id": 0})
        _ = pl.DataFrame(list(cursor))

    def time_to_polars(self):
        c = db.benchmark
        find_polars_all(c, {}, schema=self.schema, projection={"_id": 0})

    def peakmem_to_numpy(self):
        self.time_to_numpy()

    def peakmem_conventional_pandas(self):
        self.time_conventional_pandas()

    def peakmem_to_pandas(self):
        self.time_to_pandas()

    def peakmem_to_arrow(self):
        self.time_to_arrow()

    def peakmem_conventional_arrow(self):
        self.time_conventional_arrow()

    def peakmem_to_polars(self):
        self.time_to_polars()

    def peakmem_conventional_polars(self):
        self.time_conventional_polars()


class ProfileReadArray(Read):
    schema = Schema(
        {
            "x": pa.int64(),
            "y": pa.float64(),
            "emb": pa.list_(pa.float64()),
        }
    )

    def setup(self):
        coll = db.benchmark
        coll.drop()
        base_dict = dict(
            [
                ("x", 1),
                ("y", math.pi),
                ("emb", [math.pi for _ in range(EMBEDDED_OBJECT_SIZE)]),
            ]
        )
        coll.insert_many([base_dict.copy() for _ in range(N_DOCS)])
        print(
            "%d docs, %dk each with %d keys"
            % (N_DOCS, len(BSON.encode(base_dict)) // 1024, len(base_dict))
        )

    # We need this because the naive methods don't always convert nested objects.
    @staticmethod
    def exercise_table(table):
        [
            [[n for n in i.values] if isinstance(i, pa.ListScalar) else i for i in column]
            for column in table.columns
        ]

    # All of the following tests are being skipped because NumPy/Pandas do not work with nested arrays.
    def time_to_numpy(self):
        pass

    def time_to_pandas(self):
        pass

    def time_conventional_ndarray(self):
        pass

    def time_conventional_pandas(self):
        pass


class ProfileReadDocument(Read):
    schema = Schema(
        {
            "x": pa.int64(),
            "y": pa.float64(),
            "emb": pa.struct(
                [pa.field(f"a{i}", pa.float64()) for i in range(EMBEDDED_OBJECT_SIZE)]
            ),
        }
    )

    def setup(self):
        coll = db.benchmark
        coll.drop()
        base_dict = dict(
            [
                ("x", 1),
                ("y", math.pi),
                ("emb", {f"a{i}": math.pi for i in range(EMBEDDED_OBJECT_SIZE)}),
            ]
        )
        coll.insert_many([base_dict.copy() for _ in range(N_DOCS)])
        print(
            "%d docs, %dk each with %d keys"
            % (N_DOCS, len(BSON.encode(base_dict)) // 1024, len(base_dict))
        )

    # We need this because the naive methods don't always convert nested objects.
    @staticmethod
    def exercise_table(table):
        [
            [[n for n in i.values()] if isinstance(i, pa.StructScalar) else i for i in column]
            for column in table.columns
        ]

    # All of the following tests are being skipped because NumPy/Pandas do not work with nested documents.
    def time_to_numpy(self):
        pass

    def time_to_pandas(self):
        pass

    def time_conventional_ndarray(self):
        pass

    def time_conventional_pandas(self):
        pass


class ProfileReadSmall(Read):
    schema = Schema({"x": pa.int64(), "y": pa.float64()})
    dtypes = np.dtype(np.dtype([("x", np.int64), ("y", np.float64)]))

    def setup(self):
        coll = db.benchmark
        coll.drop()
        base_dict = dict(
            [
                ("x", 1),
                ("y", math.pi),
            ]
        )
        coll.insert_many([base_dict.copy() for _ in range(N_DOCS)])
        print(
            "%d docs, %dk each with %d keys"
            % (N_DOCS, len(BSON.encode(base_dict)) // 1024, len(base_dict))
        )


class ProfileReadLarge(Read):
    large_doc_keys = [f"a{i}" for i in range(LARGE_DOC_SIZE)]
    schema = Schema({k: pa.float64() for k in large_doc_keys})
    dtypes = np.dtype([(k, np.float64) for k in large_doc_keys])

    def setup(self):
        coll = db.benchmark
        coll.drop()

        base_dict = dict([(k, math.pi) for k in self.large_doc_keys])
        coll.insert_many([base_dict.copy() for _ in range(N_DOCS)])
        print(
            "%d docs, %dk each with %d keys"
            % (N_DOCS, len(BSON.encode(base_dict)) // 1024, len(base_dict))
        )


class ProfileReadExtensionSmall(Read):
    schema = Schema({"x": Decimal128Type(), "y": BinaryType(10)})
    dtypes = np.dtype(np.dtype([("x", np.object_), ("y", np.object_)]))

    def setup(self):
        coll = db.benchmark
        coll.drop()
        base_dict = dict(
            [
                ("x", Decimal128("1")),
                ("y", Binary(b"1234", 10)),
            ]
        )
        coll.insert_many([base_dict.copy() for _ in range(N_DOCS)])
        print(
            "%d docs, %dk each with %d keys"
            % (N_DOCS, len(BSON.encode(base_dict)) // 1024, len(base_dict))
        )

    # This must be skipped because arrow can't read the Decimal128Type
    def time_conventional_arrow(self):
        pass

    def time_insert_conventional(self):
        pass


class ProfileReadExtensionLarge(Read):
    large_doc_keys = [f"{i}" for i in range(LARGE_DOC_SIZE)]
    schema = Schema({k: Decimal128Type() for k in large_doc_keys})
    dtypes = np.dtype([(k, np.object_) for k in large_doc_keys])

    def setup(self):
        coll = db.benchmark
        coll.drop()

        base_dict = dict([(k, Decimal128(k)) for k in self.large_doc_keys])
        coll.insert_many([base_dict.copy() for _ in range(N_DOCS)])
        print(
            "%d docs, %dk each with %d keys"
            % (N_DOCS, len(BSON.encode(base_dict)) // 1024, len(base_dict))
        )

    # This must be skipped because arrow can't read the Decimal128Type
    def time_conventional_arrow(self):
        pass

    def time_insert_conventional(self):
        pass


class ProfileInsertSmall(Insert):
    large_doc_keys = [f"a{i}" for i in range(LARGE_DOC_SIZE)]
    schema = Schema({"x": pa.int64(), "y": pa.float64()})
    dtypes = np.dtype([("x", np.int64), ("y", np.float64)])

    def setup(self):
        coll = db.benchmark
        coll.drop()
        base_dict = dict([("x", 1), ("y", math.pi)])
        coll.insert_many([base_dict.copy() for _ in range(N_DOCS)])
        print(
            "%d docs, %dk each with %d keys"
            % (N_DOCS, len(BSON.encode(base_dict)) // 1024, len(base_dict))
        )
        self.arrow_table = find_arrow_all(db.benchmark, {}, schema=self.schema)
        self.pandas_table = find_pandas_all(db.benchmark, {}, schema=self.schema)
        self.numpy_arrays = find_numpy_all(db.benchmark, {}, schema=self.schema)
        self.polars_table = find_polars_all(db.benchmark, {}, schema=self.schema)


class ProfileInsertLarge(Insert):
    large_doc_keys = [f"a{i}" for i in range(LARGE_DOC_SIZE)]
    schema = Schema({k: pa.float64() for k in large_doc_keys})
    dtypes = np.dtype([(k, np.float64) for k in large_doc_keys])

    def setup(self):
        coll = db.benchmark
        coll.drop()
        base_dict = dict([(k, math.pi) for k in self.large_doc_keys])
        coll.insert_many([base_dict.copy() for _ in range(N_DOCS)])
        print(
            "%d docs, %dk each with %d keys"
            % (N_DOCS, len(BSON.encode(base_dict)) // 1024, len(base_dict))
        )
        self.arrow_table = find_arrow_all(db.benchmark, {}, schema=self.schema)
        self.pandas_table = find_pandas_all(db.benchmark, {}, schema=self.schema)
        self.numpy_arrays = find_numpy_all(db.benchmark, {}, schema=self.schema)
        self.polars_table = find_polars_all(db.benchmark, {}, schema=self.schema)
