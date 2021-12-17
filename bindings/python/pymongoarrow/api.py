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

from pymongoarrow.context import PyMongoArrowContext
from pymongoarrow.lib import process_bson_stream
from pymongoarrow.schema import Schema


__all__ = [
    'aggregate_arrow_all',
    'find_arrow_all',
    'aggregate_pandas_all',
    'find_pandas_all',
    'aggregate_numpy_all',
    'find_numpy_all',
    'Schema'
]


_PATCH_METHODS = [
    'aggregate_arrow_all',
    'find_arrow_all',
    'aggregate_pandas_all',
    'find_pandas_all',
    'aggregate_numpy_all',
    'find_numpy_all',
]


def find_arrow_all(collection, query, *, schema, **kwargs):
    """Method that returns the results of a find query as a
    :class:`pyarrow.Table` instance.

    :Parameters:
      - `collection`: Instance of :class:`~pymongo.collection.Collection`.
        against which to run the ``find`` operation.
      - `query`: A mapping containing the query to use for the find operation.
      - `schema`: Instance of :class:`~pymongoarrow.schema.Schema`.

    Additional keyword-arguments passed to this method will be passed
    directly to the underlying ``find`` operation.

    :Returns:
      An instance of class:`pyarrow.Table`.
    """
    context = PyMongoArrowContext.from_schema(
        schema, codec_options=collection.codec_options)

    for opt in ('cursor_type',):
        if kwargs.pop(opt, None):
            warnings.warn(
                f'Ignoring option {opt!r} as it is not supported by '
                'PyMongoArrow', UserWarning, stacklevel=2)

    kwargs.setdefault('projection', schema._get_projection())
    raw_batch_cursor = collection.find_raw_batches(
        query, **kwargs)
    for batch in raw_batch_cursor:
        process_bson_stream(batch, context)

    return context.finish()


def aggregate_arrow_all(collection, pipeline, *, schema, **kwargs):
    """Method that returns the results of an aggregation pipeline as a
    :class:`pyarrow.Table` instance.

    :Parameters:
      - `collection`: Instance of :class:`~pymongo.collection.Collection`.
        against which to run the ``aggregate`` operation.
      - `pipeline`: A list of aggregation pipeline stages.
      - `schema`: Instance of :class:`~pymongoarrow.schema.Schema`.

    Additional keyword-arguments passed to this method will be passed
    directly to the underlying ``aggregate`` operation.

    :Returns:
      An instance of class:`pyarrow.Table`.
    """
    context = PyMongoArrowContext.from_schema(
        schema, codec_options=collection.codec_options)

    if pipeline and ("$out" in pipeline[-1] or "$merge" in pipeline[-1]):
        raise ValueError(
            "Aggregation pipelines containing a '$out' or '$merge' stage are "
            "not supported by PyMongoArrow")

    for opt in ('batchSize', 'useCursor'):
        if kwargs.pop(opt, None):
            warnings.warn(
                f'Ignoring option {opt!r} as it is not supported by '
                'PyMongoArrow', UserWarning, stacklevel=2)

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


def find_pandas_all(collection, query, *, schema, **kwargs):
    """Method that returns the results of a find query as a
    :class:`pandas.DataFrame` instance.

    :Parameters:
      - `collection`: Instance of :class:`~pymongo.collection.Collection`.
        against which to run the ``find`` operation.
      - `query`: A mapping containing the query to use for the find operation.
      - `schema`: Instance of :class:`~pymongoarrow.schema.Schema`.

    Additional keyword-arguments passed to this method will be passed
    directly to the underlying ``find`` operation.

    :Returns:
      An instance of class:`pandas.DataFrame`.
    """
    return _arrow_to_pandas(
        find_arrow_all(collection, query, schema=schema, **kwargs))


def aggregate_pandas_all(collection, pipeline, *, schema, **kwargs):
    """Method that returns the results of an aggregation pipeline as a
    :class:`pandas.DataFrame` instance.

    :Parameters:
      - `collection`: Instance of :class:`~pymongo.collection.Collection`.
        against which to run the ``find`` operation.
      - `pipeline`: A list of aggregation pipeline stages.
      - `schema`: Instance of :class:`~pymongoarrow.schema.Schema`.

    Additional keyword-arguments passed to this method will be passed
    directly to the underlying ``aggregate`` operation.

    :Returns:
      An instance of class:`pandas.DataFrame`.
    """
    return _arrow_to_pandas(aggregate_arrow_all(
        collection, pipeline, schema=schema, **kwargs))


def _arrow_to_numpy(arrow_table, schema):
    """Helper function that converts an Arrow Table to a dictionary
    containing NumPy arrays. The memory buffers backing the given Arrow Table
    may be destroyed after conversion if the resulting Numpy array(s) is not a
    view on the Arrow data.

    See https://arrow.apache.org/docs/python/numpy.html for details.
    """
    container = {}
    for fname in schema:
        container[fname] = arrow_table[fname].to_numpy()
    return container


def find_numpy_all(collection, query, *, schema, **kwargs):
    """Method that returns the results of a find query as a
    :class:`dict` instance whose keys are field names and values are
    :class:`~numpy.ndarray` instances bearing the appropriate dtype.

    :Parameters:
      - `collection`: Instance of :class:`~pymongo.collection.Collection`.
        against which to run the ``find`` operation.
      - `query`: A mapping containing the query to use for the find operation.
      - `schema`: Instance of :class:`~pymongoarrow.schema.Schema`.

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
    return _arrow_to_numpy(
        find_arrow_all(collection, query, schema=schema, **kwargs), schema)


def aggregate_numpy_all(collection, pipeline, *, schema, **kwargs):
    """Method that returns the results of an aggregation pipeline as a
    :class:`dict` instance whose keys are field names and values are
    :class:`~numpy.ndarray` instances bearing the appropriate dtype.

    :Parameters:
      - `collection`: Instance of :class:`~pymongo.collection.Collection`.
        against which to run the ``find`` operation.
      - `query`: A mapping containing the query to use for the find operation.
      - `schema`: Instance of :class:`~pymongoarrow.schema.Schema`.

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
        aggregate_arrow_all(collection, pipeline, schema=schema, **kwargs), schema)
