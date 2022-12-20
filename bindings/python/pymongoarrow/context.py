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
from bson.codec_options import DEFAULT_CODEC_OPTIONS
from pyarrow import Table, timestamp
from pymongoarrow.lib import (
    BoolBuilder,
    DatetimeBuilder,
    DocumentBuilder,
    DoubleBuilder,
    Int32Builder,
    Int64Builder,
    ListBuilder,
    ObjectIdBuilder,
    StringBuilder,
)
from pymongoarrow.types import _BsonArrowTypes, _get_internal_typemap

_TYPE_TO_BUILDER_CLS = {
    _BsonArrowTypes.int32: Int32Builder,
    _BsonArrowTypes.int64: Int64Builder,
    _BsonArrowTypes.double: DoubleBuilder,
    _BsonArrowTypes.datetime: DatetimeBuilder,
    _BsonArrowTypes.objectid: ObjectIdBuilder,
    _BsonArrowTypes.decimal128_str: StringBuilder,
    _BsonArrowTypes.string: StringBuilder,
    _BsonArrowTypes.bool: BoolBuilder,
    _BsonArrowTypes.document: DocumentBuilder,
    _BsonArrowTypes.array: ListBuilder,
}


class PyMongoArrowContext:
    """A context for converting BSON-formatted data to an Arrow Table."""

    def __init__(self, schema, builder_map, codec_options=None):
        """Initialize the context.

        :Parameters:
          - `schema`: Instance of :class:`~pymongoarrow.schema.Schema`.
          - `builder_map`: Mapping of utf-8-encoded field names to
            :class:`~pymongoarrow.builders._BuilderBase` instances.
        """
        self.schema = schema
        self.builder_map = builder_map
        if self.schema is None and codec_options is not None:
            self.tzinfo = codec_options.tzinfo
        else:
            self.tzinfo = None

    @classmethod
    def from_schema(cls, schema, codec_options=DEFAULT_CODEC_OPTIONS):
        """Initialize the context from a :class:`~pymongoarrow.schema.Schema`
        instance.

        :Parameters:
          - `schema`: Instance of :class:`~pymongoarrow.schema.Schema`.
          - `codec_options` (optional): An instance of
            :class:`~bson.codec_options.CodecOptions`.
        """
        if schema is None:
            return cls(schema, {}, codec_options)

        builder_map = {}
        tzinfo = codec_options.tzinfo
        str_type_map = _get_internal_typemap(schema.typemap)
        for fname, ftype in str_type_map.items():
            builder_cls = _TYPE_TO_BUILDER_CLS[ftype]
            encoded_fname = fname.encode("utf-8")

            # special-case initializing builders for parameterized types
            if builder_cls == DatetimeBuilder:
                arrow_type = schema.typemap[fname]
                if tzinfo is not None and arrow_type.tz is None:
                    arrow_type = timestamp(arrow_type.unit, tz=tzinfo)
                builder_map[encoded_fname] = DatetimeBuilder(dtype=arrow_type)
            elif builder_cls == DocumentBuilder:
                arrow_type = schema.typemap[fname]
                builder_map[encoded_fname] = DocumentBuilder(arrow_type, tzinfo)
            elif builder_cls == ListBuilder:
                arrow_type = schema.typemap[fname]
                builder_map[encoded_fname] = ListBuilder(arrow_type, tzinfo)

            else:
                builder_map[encoded_fname] = builder_cls()
        return cls(schema, builder_map)

    def finish(self):
        arrays = []
        names = []
        for fname, builder in self.builder_map.items():
            arrays.append(builder.finish())
            names.append(fname.decode("utf-8"))
        return Table.from_arrays(arrays=arrays, names=names)
