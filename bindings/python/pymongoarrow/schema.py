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

import pyarrow as pa

from pymongoarrow.types import _normalize_typeid


class Schema:
    """A mapping of field names to data types.

    To create a schema, provide its constructor a mapping of field names
    to their expected types, e.g.::

       schema1 = Schema({'field_1': int, 'field_2': float})

    Each key in ``schema`` is a field name and its corresponding value
    is the expected type of the data contained in the named field.

    For more examples, see :ref:`schema usage`.

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
            msg = "schema must be a mapping or sequence"
            raise ValueError(msg)
        self.typemap = normed

    def __iter__(self):
        yield from self.typemap

    def __repr__(self):
        return f"<{self.__class__.__name__} {self.typemap!r}>"

    @staticmethod
    def _normalize_mapping(mapping):
        normed = {}
        for fname, ftype in mapping.items():
            normed[fname] = _normalize_typeid(ftype, fname)
        return normed

    def _get_projection(self):
        projection = {"_id": False}
        for fname, ftype in self.typemap.items():
            projection[fname] = self._get_field_projection_value(ftype)
        return projection

    def _get_field_projection_value(self, ftype):
        value = True
        if isinstance(ftype, pa.ListType):
            return self._get_field_projection_value(ftype.value_field.type)
        if isinstance(ftype, pa.StructType):
            projection = {}
            for nested_ftype in ftype:
                projection[nested_ftype.name] = True
            value = projection
        return value

    def __eq__(self, other):
        if isinstance(other, type(self)):
            return self.typemap == other.typemap
        return False

    @classmethod
    def from_arrow(cls, aschema: pa.Schema):
        """Create a :class:`~pymongoarrow.schema.Schema` instance from a :class:`~pyarrow.Schema`

        :Parameters:
          - `aschema`: PyArrow Schema
        """
        self = cls({})
        for field in aschema:
            self.typemap[field.name] = field.type
        return self

    def to_arrow(self):
        """Output the Schema as an instance of class:`~pyarrow.Schema`."""
        fields = []
        for name, type_ in self.typemap.items():
            fields.append(pa.field(name=name, type=type_))
        return pa.schema(fields)
