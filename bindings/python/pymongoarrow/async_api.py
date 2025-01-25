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

from __future__ import annotations

import warnings
from typing import TYPE_CHECKING

import pandas as pd

try:
    import polars as pl
except ImportError:
    pl = None

import pymongo.errors
from bson import encode
from bson.codec_options import TypeRegistry
from bson.raw_bson import RawBSONDocument
from numpy import ndarray
from pyarrow import Schema as ArrowSchema
from pyarrow import Table, timestamp
from pyarrow.types import is_date32, is_date64

from pymongoarrow.api import (
    _MAX_MESSAGE_SIZE,
    _MAX_WRITE_BATCH_SIZE,
    _arrow_to_numpy,
    _arrow_to_pandas,
    _arrow_to_polars,
    _PandasNACodec,
    _tabular_generator,
    _transform_bwe,
)
from pymongoarrow.context import PyMongoArrowContext
from pymongoarrow.errors import ArrowWriteError
from pymongoarrow.result import ArrowWriteResult
from pymongoarrow.types import _validate_schema

if TYPE_CHECKING:
    from pymongo.asynchronous.collection import AsyncCollection


async def find_arrow_all(collection: AsyncCollection, query, *, schema=None, **kwargs):
    """Method that returns the results of a find query as a
    :class:`pyarrow.Table` instance.

    :Parameters:
      - `collection`: Instance of :class:`~pymongo.asynchronous.collection.AsyncCollection`.
        against which to run the ``find`` operation.
      - `query`: A mapping containing the query to use for the find operation.
      - `schema` (optional): Instance of :class:`~pymongoarrow.schema.Schema`.
        If the schema is not given, it will be inferred using the data in the
        result set.

    Additional keyword-arguments passed to this method will be passed
    directly to the underlying ``find`` operation.

    :Returns:
      An instance of class:`pyarrow.Table`.
    """
    context = PyMongoArrowContext(schema, codec_options=collection.codec_options)

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
    async for batch in raw_batch_cursor:
        context.process_bson_stream(batch)

    return context.finish()


async def aggregate_arrow_all(collection: AsyncCollection, pipeline, *, schema=None, **kwargs):
    """Method that returns the results of an aggregation pipeline as a
    :class:`pyarrow.Table` instance.

    :Parameters:
      - `collection`: Instance of :class:`~pymongo.asynchronous.collection.AsyncCollection`.
        against which to run the ``aggregate`` operation.
      - `pipeline`: A list of aggregation pipeline stages.
      - `schema` (optional): Instance of :class:`~pymongoarrow.schema.Schema`.
        If the schema is not given, it will be inferred using the data in the
        result set.

    Additional keyword-arguments passed to this method will be passed
    directly to the underlying ``aggregate`` operation.

    :Returns:
      An instance of class:`pyarrow.Table`.
    """
    context = PyMongoArrowContext(schema, codec_options=collection.codec_options)

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

    raw_batch_cursor = await collection.aggregate_raw_batches(pipeline, **kwargs)
    async for batch in raw_batch_cursor:
        context.process_bson_stream(batch)

    return context.finish()


async def find_pandas_all(collection: AsyncCollection, query, *, schema=None, **kwargs):
    """Method that returns the results of a find query as a
    :class:`pandas.DataFrame` instance.

    :Parameters:
      - `collection`: Instance of :class:`~pymongo.asynchronous.collection.AsyncCollection`.
        against which to run the ``find`` operation.
      - `query`: A mapping containing the query to use for the find operation.
      - `schema` (optional): Instance of :class:`~pymongoarrow.schema.Schema`.
        If the schema is not given, it will be inferred using the data in the
        result set.

    Additional keyword-arguments passed to this method will be passed
    directly to the underlying ``find`` operation.

    :Returns:
      An instance of class:`pandas.DataFrame`.
    """
    return _arrow_to_pandas(await find_arrow_all(collection, query, schema=schema, **kwargs))


async def aggregate_pandas_all(collection: AsyncCollection, pipeline, *, schema=None, **kwargs):
    """Method that returns the results of an aggregation pipeline as a
    :class:`pandas.DataFrame` instance.

    :Parameters:
      - `collection`: Instance of :class:`~pymongo.asynchronous.collection.AsyncCollection`.
        against which to run the ``find`` operation.
      - `pipeline`: A list of aggregation pipeline stages.
      - `schema` (optional): Instance of :class:`~pymongoarrow.schema.Schema`.
        If the schema is not given, it will be inferred using the data in the
        result set.

    Additional keyword-arguments passed to this method will be passed
    directly to the underlying ``aggregate`` operation.

    :Returns:
      An instance of class:`pandas.DataFrame`.
    """
    return _arrow_to_pandas(
        await aggregate_arrow_all(collection, pipeline, schema=schema, **kwargs)
    )


async def find_numpy_all(collection: AsyncCollection, query, *, schema=None, **kwargs):
    """Method that returns the results of a find query as a
    :class:`dict` instance whose keys are field names and values are
    :class:`~numpy.ndarray` instances bearing the appropriate dtype.

    :Parameters:
      - `collection`: Instance of :class:`~pymongo.asynchronous.collection.AsyncCollection`.
        against which to run the ``find`` operation.
      - `query`: A mapping containing the query to use for the find operation.
      - `schema` (optional): Instance of :class:`~pymongoarrow.schema.Schema`.
        If the schema is not given, it will be inferred using the data in the
        result set.

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
    return _arrow_to_numpy(await find_arrow_all(collection, query, schema=schema, **kwargs), schema)


async def aggregate_numpy_all(collection: AsyncCollection, pipeline, *, schema=None, **kwargs):
    """Method that returns the results of an aggregation pipeline as a
    :class:`dict` instance whose keys are field names and values are
    :class:`~numpy.ndarray` instances bearing the appropriate dtype.

    :Parameters:
      - `collection`: Instance of :class:`~pymongo.asynchronous.collection.AsyncCollection`.
        against which to run the ``find`` operation.
      - `query`: A mapping containing the query to use for the find operation.
      - `schema` (optional): Instance of :class:`~pymongoarrow.schema.Schema`.
        If the schema is not given, it will be inferred using the data in the
        result set.

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
        await aggregate_arrow_all(collection, pipeline, schema=schema, **kwargs), schema
    )


async def find_polars_all(collection: AsyncCollection, query, *, schema=None, **kwargs):
    """Method that returns the results of a find query as a
    :class:`polars.DataFrame` instance.

    :Parameters:
      - `collection`: Instance of :class:`~pymongo.asynchronous.collection.AsyncCollection`.
        against which to run the ``find`` operation.
      - `query`: A mapping containing the query to use for the find operation.
      - `schema` (optional): Instance of :class:`~pymongoarrow.schema.Schema`.
        If the schema is not given, it will be inferred using the data in the
        result set.

    Additional keyword-arguments passed to this method will be passed
    directly to the underlying ``find`` operation.

    :Returns:
      An instance of class:`polars.DataFrame`.

    .. versionadded:: 1.3
    """
    return _arrow_to_polars(await find_arrow_all(collection, query, schema=schema, **kwargs))


async def aggregate_polars_all(collection: AsyncCollection, pipeline, *, schema=None, **kwargs):
    """Method that returns the results of an aggregation pipeline as a
    :class:`polars.DataFrame` instance.

    :Parameters:
      - `collection`: Instance of :class:`~pymongo.asynchronous.collection.AsyncCollection`.
        against which to run the ``find`` operation.
      - `pipeline`: A list of aggregation pipeline stages.
      - `schema` (optional): Instance of :class:`~pymongoarrow.schema.Schema`.
        If the schema is not given, it will be inferred using the data in the
        result set.

    Additional keyword-arguments passed to this method will be passed
    directly to the underlying ``aggregate`` operation.

    :Returns:
      An instance of class:`polars.DataFrame`.
    """
    return _arrow_to_polars(
        await aggregate_arrow_all(collection, pipeline, schema=schema, **kwargs)
    )


async def write(collection: AsyncCollection, tabular, *, exclude_none: bool = False):
    """Write data from `tabular` into the given MongoDB `collection`.

    :Parameters:
      - `collection`: Instance of :class:`~pymongo.collection.Collection`.
        against which to run the operation.
      - `tabular`: A tabular data store to use for the write operation.
      - `exclude_none`: Whether to skip writing `null` fields in documents.

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

    tabular_gen = _tabular_generator(tabular, exclude_none=exclude_none)

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
            await collection.insert_many(cur_batch)
        except pymongo.errors.BulkWriteError as bwe:
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
