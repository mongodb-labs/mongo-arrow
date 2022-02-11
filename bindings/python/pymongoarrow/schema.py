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
import collections.abc as abc

from pymongoarrow.types import _normalize_typeid


class Schema:
    """A mapping of field names to data types.

    To create a schema, provide its constructor a mapping of field names
    to their expected types, e.g.::

       schema1 = Schema({'field_1': int, 'field_2': float})

    Each key in ``schema`` is a field name and its corresponding value
    is the expected type of the data contained in the named field.

    Data types can be specified as pyarrow type instances (e.g.
    an instance of :class:`pyarrow.int64`), bson types (e.g.
    :class:`bson.Int64`), or python type-identifiers (e.g. ``int``,
    ``float``). To see a complete list of supported data types and their
    corresponding type-identifiers, see :ref:`type support`.
    """

    def __init__(self, schema):
        """Create a :class:`~pymongoarrow.schema.Schema` instance from a
        mapping or an iterable.

        :Parameters:
          - `schema`: A mapping.
        """
        if isinstance(schema, abc.Mapping):
            normed = type(self)._normalize_mapping(schema)
        else:
            raise ValueError("schema must be a mapping or sequence")
        self.typemap = normed

    def __iter__(self):
        for fname in self.typemap:
            yield fname

    @staticmethod
    def _normalize_mapping(mapping):
        normed = {}
        for fname, ftype in mapping.items():
            normed[fname] = _normalize_typeid(ftype, fname)
        return normed

    def _get_projection(self):
        projection = {"_id": False}
        for fname, _ in self.typemap.items():
            projection[fname] = True
        return projection

    def __eq__(self, other):
        if isinstance(other, type(self)):
            return self.typemap == other.typemap
        return False
