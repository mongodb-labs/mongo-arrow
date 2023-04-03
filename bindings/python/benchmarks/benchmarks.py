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
from abc import ABC, abstractmethod

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

N_DOCS = int(os.environ.get("N_DOCS"))
name_to_obj = {"list": list, "dict": dict}
assert pymongo.has_c()
db = pymongo.MongoClient().pymongoarrow_test


class Insert(ABC):
    """
    A benchmark that times the performance of various kinds
    of inserting tabular data.
    """

    @abstractmethod
    def setup(self):
        raise NotImplementedError

    @abstractmethod
    def time_insert_arrow(self):
        write(db.benchmark, self.arrow_table)

    @abstractmethod
    def time_insert_conventional(self):
        tab = self.arrow_table.to_pylist()
        db.benchmark.insert_many(tab)

    @abstractmethod
    def time_insert_pandas(self):
        write(db.benchmark, self.pandas_table)

    @abstractmethod
    def time_insert_numpy(self):
        write(db.benchmark, self.numpy_arrays)


class Read(ABC):
    """
    A benchmark that times the performance of various kinds
    of reading MongoDB data.
    """

    def setup(self):
        raise NotImplementedError

    # We need this because the naive methods don't always convert nested objects.
    @staticmethod
    def exercise_table(table):
        pass

    @abstractmethod
    def time_conventional_ndarray(self):
        collection = db.benchmark
        cursor = collection.find()
        dtype = self.dtypes
        if "Large" in type(self).__name__:
            np.array([tuple(doc[k] for k in self.large_doc_keys) for doc in cursor], dtype=dtype)
        else:
            np.array([(doc["x"], doc["y"]) for doc in cursor], dtype=dtype)

    @abstractmethod
    def time_to_numpy(self):
        c = db.benchmark
        find_numpy_all(c, {}, schema=self.schema)

    @abstractmethod
    def time_conventional_pandas(self):
        collection = db.benchmark
        cursor = collection.find(projection={"_id": 0})
        _ = pd.DataFrame(list(cursor))

    @abstractmethod
    def time_to_pandas(self):
        c = db.benchmark
        find_pandas_all(c, {}, schema=self.schema, projection={"_id": 0})

    @abstractmethod
    def time_to_arrow(self):
        c = db.benchmark
        table = find_arrow_all(c, {}, schema=self.schema)
        self.exercise_table(table)

    @abstractmethod
    def time_conventional_arrow(self):
        c = db.benchmark
        f = list(c.find({}, projection={"_id": 0}))
        table = pyarrow.Table.from_pylist(f)
        self.exercise_table(table)


class ProfileReadArray(Read):
    def setup(self):
        coll = db.benchmark
        coll.drop()
        base_dict = collections.OrderedDict(
            [("x", 1), ("y", math.pi), ("emb", [math.pi for _ in range(64)])]
        )
        schema_dict = {"x": pyarrow.int64(), "y": pyarrow.float64()}
        dtypes_list = np.dtype([("x", np.int64), ("y", np.float64)])
        self.schema = Schema(schema_dict)
        self.dtypes = np.dtype(dtypes_list)
        coll.insert_many([base_dict.copy() for _ in range(N_DOCS)])
        print(
            "%d docs, %dk each with %d keys"
            % (N_DOCS, len(BSON.encode(base_dict)) // 1024, len(base_dict))
        )

    # We need this because the naive methods don't always convert nested objects.
    @staticmethod
    def exercise_table(table):
        [
            [[n for n in i.values] if isinstance(i, pyarrow.ListScalar) else i for i in column]
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


class ProfileReadSmall(Read):
    def setup(self):
        coll = db.benchmark
        coll.drop()
        base_dict = collections.OrderedDict(
            [
                ("x", 1),
                ("y", math.pi),
            ]
        )
        schema_dict = {"x": pyarrow.int64(), "y": pyarrow.float64()}
        dtypes_list = np.dtype([("x", np.int64), ("y", np.float64)])
        self.schema = Schema(schema_dict)
        self.dtypes = np.dtype(dtypes_list)
        coll.insert_many([base_dict.copy() for _ in range(N_DOCS)])
        print(
            "%d docs, %dk each with %d keys"
            % (N_DOCS, len(BSON.encode(base_dict)) // 1024, len(base_dict))
        )


class ProfileReadLarge(Read):
    def setup(self):
        coll = db.benchmark
        coll.drop()
        large_doc_keys = self.large_doc_keys = [f"a{i}" for i in range(2600)]
        base_dict = collections.OrderedDict([(k, math.pi) for k in large_doc_keys])
        dtypes_list = np.dtype([(k, np.float64) for k in large_doc_keys])
        schema_dict = {k: pyarrow.float64() for k in large_doc_keys}
        self.schema = Schema(schema_dict)
        self.dtypes = np.dtype(dtypes_list)
        coll.insert_many([base_dict.copy() for _ in range(N_DOCS)])
        print(
            "%d docs, %dk each with %d keys"
            % (N_DOCS, len(BSON.encode(base_dict)) // 1024, len(base_dict))
        )


class ProfileInsertSmall(Insert):
    def setup(self):
        coll = db.benchmark
        coll.drop()
        base_dict = collections.OrderedDict([("x", 1), ("y", math.pi)])
        dtypes_list = np.dtype([("x", np.int64), ("y", np.float64)])
        self.dtypes = np.dtype(dtypes_list)
        coll.insert_many([base_dict.copy() for _ in range(N_DOCS)])
        print(
            "%d docs, %dk each with %d keys"
            % (N_DOCS, len(BSON.encode(base_dict)) // 1024, len(base_dict))
        )
        schema = Schema({"x": pyarrow.int64(), "y": pyarrow.float64()})

        self.arrow_table = find_arrow_all(db.benchmark, {}, schema=schema)
        self.pandas_table = find_pandas_all(db.benchmark, {}, schema=schema)
        self.numpy_arrays = find_numpy_all(db.benchmark, {}, schema=schema)


class ProfileInsertLarge(Insert):
    def setup(self):
        coll = db.benchmark
        coll.drop()
        large_doc_keys = [f"a{i}" for i in range(2600)]
        base_dict = collections.OrderedDict([(k, math.pi) for k in large_doc_keys])
        dtypes_list = np.dtype([(k, np.float64) for k in large_doc_keys])
        self.dtypes = np.dtype(dtypes_list)
        coll.insert_many([base_dict.copy() for _ in range(N_DOCS)])
        print(
            "%d docs, %dk each with %d keys"
            % (N_DOCS, len(BSON.encode(base_dict)) // 1024, len(base_dict))
        )
        schema = Schema({k: pyarrow.float64() for k in large_doc_keys})
        self.arrow_table = find_arrow_all(db.benchmark, {}, schema=schema)
        self.pandas_table = find_pandas_all(db.benchmark, {}, schema=schema)
        self.numpy_arrays = find_numpy_all(db.benchmark, {}, schema=schema)
