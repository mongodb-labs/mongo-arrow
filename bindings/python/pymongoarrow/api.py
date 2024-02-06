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
import warnings

import numpy as np
import pandas as pd

try:
    import polars as pl
except ImportError:
    pl = None

import pyarrow as pa
import pymongo.errors
from bson import encode
from bson.codec_options import TypeEncoder, TypeRegistry
from bson.raw_bson import RawBSONDocument
from numpy import ndarray
from pyarrow import Schema as ArrowSchema
from pyarrow import Table, timestamp
from pyarrow.types import is_date32, is_date64
from pymongo.bulk import BulkWriteError
from pymongo.common import MAX_WRITE_BATCH_SIZE

from pymongoarrow.context import PyMongoArrowContext
from pymongoarrow.errors import ArrowWriteError
from pymongoarrow.result import ArrowWriteResult
from pymongoarrow.schema import Schema
from pymongoarrow.types import _validate_schema, get_numpy_type

try:  # noqa: SIM105
    from pymongoarrow.lib import process_bson_stream
except ImportError:
    pass

__all__ = [
    "aggregate_arrow_all",
    "find_arrow_all",
    "aggregate_pandas_all",
    "find_pandas_all",
    "aggregate_numpy_all",
    "find_numpy_all",
    "aggregate_polars_all",
    "find_polars_all",
    "write",
    "Schema",
]


_PATCH_METHODS = [
    "aggregate_arrow_all",
    "find_arrow_all",
    "aggregate_pandas_all",
    "find_pandas_all",
    "aggregate_numpy_all",
    "find_numpy_all",
    "aggregate_polars_all",
    "find_polars_all",
]

# MongoDB 3.6's maxMessageSizeBytes minus some overhead to account
# for the command plus OP_MSG.
_MAX_MESSAGE_SIZE = 48000000 - 16 * 1024
# The maximum number of bulk write operations in one batch.
_MAX_WRITE_BATCH_SIZE = max(100000, MAX_WRITE_BATCH_SIZE)


def find_arrow_all(collection, query, *, schema=None, **kwargs):
    """Method that returns the results of a find query as a
    :class:`pyarrow.Table` instance.

    :Parameters:
      - `collection`: Instance of :class:`~pymongo.collection.Collection`.
        against which to run the ``find`` operation.
      - `query`: A mapping containing the query to use for the find operation.
      - `schema` (optional): Instance of :class:`~pymongoarrow.schema.Schema`.
        If the schema is not given, it will be inferred using the first
        document in the result set.

    Additional keyword-arguments passed to this method will be passed
    directly to the underlying ``find`` operation.

    :Returns:
      An instance of class:`pyarrow.Table`.
    """
    context = PyMongoArrowContext.from_schema(schema, codec_options=collection.codec_options)

    for opt in ("cursor_type",):
        if kwargs.pop(opt, None):
            warnings.warn(
                f"Ignoring option {opt!r} as it is not supported by PyMongoArrow",
                UserWarning,
                stacklevel=2,
            )

    if schema:
        kwargs.setdefault("projection", schema._get_projection())

    raw_batch_cursor = collection.find_raw_batches(query, **kwargs)
    for batch in raw_batch_cursor:
        process_bson_stream(batch, context)

    return context.finish()


def aggregate_arrow_all(collection, pipeline, *, schema=None, **kwargs):
    """Method that returns the results of an aggregation pipeline as a
    :class:`pyarrow.Table` instance.

    :Parameters:
      - `collection`: Instance of :class:`~pymongo.collection.Collection`.
        against which to run the ``aggregate`` operation.
      - `pipeline`: A list of aggregation pipeline stages.
      - `schema` (optional): Instance of :class:`~pymongoarrow.schema.Schema`.
        If the schema is not given, it will be inferred using the first
        document in the result set.

    Additional keyword-arguments passed to this method will be passed
    directly to the underlying ``aggregate`` operation.

    :Returns:
      An instance of class:`pyarrow.Table`.
    """
    context = PyMongoArrowContext.from_schema(schema, codec_options=collection.codec_options)

    if pipeline and ("$out" in pipeline[-1] or "$merge" in pipeline[-1]):
        msg = (
            "Aggregation pipelines containing a '$out' or '$merge' stage are "
            "not supported by PyMongoArrow"
        )
        raise ValueError(msg)

    for opt in ("batchSize", "useCursor"):
        if kwargs.pop(opt, None):
            warnings.warn(
                f"Ignoring option {opt!r} as it is not supported by PyMongoArrow",
                UserWarning,
                stacklevel=2,
            )
    if schema:
        pipeline.append({"$project": schema._get_projection()})

    raw_batch_cursor = collection.aggregate_raw_batches(pipeline, **kwargs)
    for batch in raw_batch_cursor:
        process_bson_stream(batch, context)

    return context.finish()


def _arrow_to_pandas(arrow_table):
    """Helper function that converts an Arrow Table to a Pandas DataFrame
    while minimizing peak memory consumption during conversion. The memory
    buffers backing the given Arrow Table are also destroyed after conversion.

    See https://arrow.apache.org/docs/python/pandas.html#reducing-memory-use-in-table-to-pandas
    for details.
    """
    return arrow_table.to_pandas(split_blocks=True, self_destruct=True)


def find_pandas_all(collection, query, *, schema=None, **kwargs):
    """Method that returns the results of a find query as a
    :class:`pandas.DataFrame` instance.

    :Parameters:
      - `collection`: Instance of :class:`~pymongo.collection.Collection`.
        against which to run the ``find`` operation.
      - `query`: A mapping containing the query to use for the find operation.
      - `schema` (optional): Instance of :class:`~pymongoarrow.schema.Schema`.
        If the schema is not given, it will be inferred using the first
        document in the result set.

    Additional keyword-arguments passed to this method will be passed
    directly to the underlying ``find`` operation.

    :Returns:
      An instance of class:`pandas.DataFrame`.
    """
    return _arrow_to_pandas(find_arrow_all(collection, query, schema=schema, **kwargs))


def aggregate_pandas_all(collection, pipeline, *, schema=None, **kwargs):
    """Method that returns the results of an aggregation pipeline as a
    :class:`pandas.DataFrame` instance.

    :Parameters:
      - `collection`: Instance of :class:`~pymongo.collection.Collection`.
        against which to run the ``find`` operation.
      - `pipeline`: A list of aggregation pipeline stages.
      - `schema` (optional): Instance of :class:`~pymongoarrow.schema.Schema`.
        If the schema is not given, it will be inferred using the first
        document in the result set.

    Additional keyword-arguments passed to this method will be passed
    directly to the underlying ``aggregate`` operation.

    :Returns:
      An instance of class:`pandas.DataFrame`.
    """
    return _arrow_to_pandas(aggregate_arrow_all(collection, pipeline, schema=schema, **kwargs))


def _arrow_to_numpy(arrow_table, schema=None):
    """Helper function that converts an Arrow Table to a dictionary
    containing NumPy arrays. The memory buffers backing the given Arrow Table
    may be destroyed after conversion if the resulting Numpy array(s) is not a
    view on the Arrow data.

    See https://arrow.apache.org/docs/python/numpy.html for details.
    """
    container = {}
    schema = {i.name: i.type for i in arrow_table.schema} if not schema else schema.typemap

    for fname in schema:
        dtype = get_numpy_type(schema[fname])
        if dtype == np.str_:
            container[fname] = arrow_table[fname].to_pandas().to_numpy(dtype=dtype)
        else:
            container[fname] = arrow_table[fname].to_numpy()
    return container


def find_numpy_all(collection, query, *, schema=None, **kwargs):
    """Method that returns the results of a find query as a
    :class:`dict` instance whose keys are field names and values are
    :class:`~numpy.ndarray` instances bearing the appropriate dtype.

    :Parameters:
      - `collection`: Instance of :class:`~pymongo.collection.Collection`.
        against which to run the ``find`` operation.
      - `query`: A mapping containing the query to use for the find operation.
      - `schema` (optional): Instance of :class:`~pymongoarrow.schema.Schema`.
        If the schema is not given, it will be inferred using the first
        document in the result set.

    Additional keyword-arguments passed to this method will be passed
    directly to the underlying ``find`` operation.

    This method attempts to create each NumPy array as a view on the Arrow
    data corresponding to each field in the result set. When this is not
    possible, the underlying data is copied into a new NumPy array. See
    :meth:`pyarrow.Array.to_numpy` for more information.

    NumPy arrays returned by this method that are views on Arrow data
    are not writable. Users seeking to modify such arrays must first
    create an editable copy using :meth:`numpy.copy`.

    :Returns:
      An instance of :class:`dict`.
    """
    return _arrow_to_numpy(find_arrow_all(collection, query, schema=schema, **kwargs), schema)


def aggregate_numpy_all(collection, pipeline, *, schema=None, **kwargs):
    """Method that returns the results of an aggregation pipeline as a
    :class:`dict` instance whose keys are field names and values are
    :class:`~numpy.ndarray` instances bearing the appropriate dtype.

    :Parameters:
      - `collection`: Instance of :class:`~pymongo.collection.Collection`.
        against which to run the ``find`` operation.
      - `query`: A mapping containing the query to use for the find operation.
      - `schema` (optional): Instance of :class:`~pymongoarrow.schema.Schema`.
        If the schema is not given, it will be inferred using the first
        document in the result set.

    Additional keyword-arguments passed to this method will be passed
    directly to the underlying ``aggregate`` operation.

    This method attempts to create each NumPy array as a view on the Arrow
    data corresponding to each field in the result set. When this is not
    possible, the underlying data is copied into a new NumPy array. See
    :meth:`pyarrow.Array.to_numpy` for more information.

    NumPy arrays returned by this method that are views on Arrow data
    are not writable. Users seeking to modify such arrays must first
    create an editable copy using :meth:`numpy.copy`.

    :Returns:
      An instance of :class:`dict`.
    """
    return _arrow_to_numpy(
        aggregate_arrow_all(collection, pipeline, schema=schema, **kwargs), schema
    )


def _cast_away_extension_types_on_array(array: pa.Array) -> pa.Array:
    """Return an Array where ExtensionTypes have been cast to their base pyarrow types"""
    if isinstance(array.type, pa.ExtensionType):
        return array.cast(array.type.storage_type)
    # elif pa.types.is_struct(field.type):
    #     ...
    # elif pa.types.is_list(field.type):
    #     ...
    return array


def _cast_away_extension_types_on_table(table: pa.Table) -> pa.Table:
    """Given arrow_table that may ExtensionTypes, cast these to the base pyarrow types"""
    # Convert all fields in the Arrow table
    converted_fields = [
        _cast_away_extension_types_on_array(table.column(i)) for i in range(table.num_columns)
    ]
    # Reconstruct the Arrow table
    return pa.Table.from_arrays(converted_fields, names=table.column_names)


def _arrow_to_polars(arrow_table):
    """Helper function that converts an Arrow Table to a Polars DataFrame.

    Note: Polars lacks ExtensionTypes. We cast them  to their base arrow classes.
    """
    if pl is None:
        msg = "polars is not installed. Try pip install polars."
        raise ValueError(msg)
    arrow_table_without_extensions = _cast_away_extension_types_on_table(arrow_table)
    return pl.from_arrow(arrow_table_without_extensions)


def find_polars_all(collection, query, *, schema=None, **kwargs):
    """Method that returns the results of a find query as a
    :class:`polars.DataFrame` instance.

    :Parameters:
      - `collection`: Instance of :class:`~pymongo.collection.Collection`.
        against which to run the ``find`` operation.
      - `query`: A mapping containing the query to use for the find operation.
      - `schema` (optional): Instance of :class:`~pymongoarrow.schema.Schema`.
        If the schema is not given, it will be inferred using the first
        document in the result set.

    Additional keyword-arguments passed to this method will be passed
    directly to the underlying ``find`` operation.

    :Returns:
      An instance of class:`polars.DataFrame`.

    .. versionadded:: 1.3
    """
    return _arrow_to_polars(find_arrow_all(collection, query, schema=schema, **kwargs))


def aggregate_polars_all(collection, pipeline, *, schema=None, **kwargs):
    """Method that returns the results of an aggregation pipeline as a
    :class:`polars.DataFrame` instance.

    :Parameters:
      - `collection`: Instance of :class:`~pymongo.collection.Collection`.
        against which to run the ``find`` operation.
      - `pipeline`: A list of aggregation pipeline stages.
      - `schema` (optional): Instance of :class:`~pymongoarrow.schema.Schema`.
        If the schema is not given, it will be inferred using the first
        document in the result set.

    Additional keyword-arguments passed to this method will be passed
    directly to the underlying ``aggregate`` operation.

    :Returns:
      An instance of class:`polars.DataFrame`.
    """
    return _arrow_to_polars(aggregate_arrow_all(collection, pipeline, schema=schema, **kwargs))


def _transform_bwe(bwe, offset):
    bwe["nInserted"] += offset
    for i in bwe["writeErrors"]:
        i["index"] += offset
    return {
        "writeErrors": bwe["writeErrors"],
        "nInserted": bwe["nInserted"],
        "writeConcernErrors": bwe["writeConcernErrors"],
    }


def _tabular_generator(tabular):
    if isinstance(tabular, Table):
        for i in tabular.to_batches():
            for row in i.to_pylist():
                yield row
    elif isinstance(tabular, pd.DataFrame):
        for row in tabular.to_dict("records"):
            yield row
    elif pl is not None and isinstance(tabular, pl.DataFrame):
        yield from _tabular_generator(tabular.to_arrow())
    elif isinstance(tabular, dict):
        iter_dict = {k: np.nditer(v) for k, v in tabular.items()}
        try:
            while True:
                yield {k: next(i).item() for k, i in iter_dict.items()}
        except StopIteration:
            return


class _PandasNACodec(TypeEncoder):
    """A custom type codec for Pandas NA objects."""

    @property
    def python_type(self):
        return pd.NA.__class__

    def transform_python(self, _):
        """Transform an NA object into 'None'"""
        return


def write(collection, tabular):
    """Write data from `tabular` into the given MongoDB `collection`.

    :Parameters:
      - `collection`: Instance of :class:`~pymongo.collection.Collection`.
        against which to run the operation.
      - `tabular`: A tabular data store to use for the write operation.

    :Returns:
      An instance of :class:`result.ArrowWriteResult`.
    """
    cur_offset = 0
    results = {
        "insertedCount": 0,
    }
    tab_size = len(tabular)
    if isinstance(tabular, Table):
        # Convert date objects to datetime objects.
        changed = False
        new_types = []
        for dtype in tabular.schema.types:
            if is_date32(dtype) or is_date64(dtype):
                changed = True
                dtype = timestamp("ms")  # noqa: PLW2901
            new_types.append(dtype)
        if changed:
            cols = [tabular.column(i).cast(new_types[i]) for i in range(tabular.num_columns)]
            tabular = Table.from_arrays(cols, names=tabular.column_names)
        _validate_schema(tabular.schema.types)
    elif isinstance(tabular, pd.DataFrame):
        _validate_schema(ArrowSchema.from_pandas(tabular).types)
    elif pl is not None and isinstance(tabular, pl.DataFrame):
        tabular = tabular.to_arrow()  # zero-copy in most cases and done in tabular_gen anyway
        _validate_schema(tabular.schema.types)
    elif (
        isinstance(tabular, dict)
        and len(tabular.values()) >= 1
        and ndarray is not None
        and all(isinstance(i, ndarray) for i in tabular.values())
    ):
        _validate_schema([i.dtype for i in tabular.values()])
        tab_size = len(next(iter(tabular.values())))
    else:
        msg = (
            f"Invalid tabular data object of type {type(tabular)} \n"
            "Please ensure that it is one of the supported types: "
            "DataFrame, Table, or a dictionary containing NumPy arrays."
        )
        raise ValueError(msg)

    tabular_gen = _tabular_generator(tabular)

    # Handle Pandas NA objects.
    codec_options = collection.codec_options
    type_registry = TypeRegistry([_PandasNACodec()])
    codec_options = codec_options.with_options(type_registry=type_registry)

    while cur_offset < tab_size:
        cur_size = 0
        cur_batch = []
        i = 0
        while (
            cur_size <= _MAX_MESSAGE_SIZE
            and len(cur_batch) <= _MAX_WRITE_BATCH_SIZE
            and cur_offset + i < tab_size
        ):
            enc_tab = RawBSONDocument(encode(next(tabular_gen), codec_options=codec_options))
            cur_batch.append(enc_tab)
            cur_size += len(enc_tab.raw)
            i += 1
        try:
            collection.insert_many(cur_batch)
        except BulkWriteError as bwe:
            raise ArrowWriteError(_transform_bwe(dict(bwe.details), cur_offset)) from bwe
        except pymongo.errors.PyMongoError as pme:
            raise ArrowWriteError(
                {
                    "writeErrors": [{"errmsg": str(pme), "index": cur_offset}],
                    "nInserted": cur_offset,
                    "writeConcernErrors": [],
                }
            ) from pme
        results["insertedCount"] += i
        cur_offset += i

    return ArrowWriteResult(results)
