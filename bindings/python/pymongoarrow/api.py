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
    context = PyMongoArrowContext.from_schema(schema)

    for opt in ('session', 'cursor_type', 'projection'):
        if kwargs.pop(opt, None):
            warnings.warn(
                f'Ignoring option {opt!r} as it is not supported by '
                'PyMongoArrow', UserWarning, stacklevel=2)

    kwargs['projection'] = schema._get_projection()
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
    context = PyMongoArrowContext.from_schema(schema)

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
