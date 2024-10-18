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
from pyarrow import ListArray, StructArray, Table, timestamp
from pyarrow.types import is_struct

from pymongoarrow.types import _BsonArrowTypes, _get_internal_typemap

try:
    from pymongoarrow.lib import (
        BinaryBuilder,
        BoolBuilder,
        CodeBuilder,
        Date32Builder,
        Date64Builder,
        DatetimeBuilder,
        Decimal128Builder,
        DocumentBuilder,
        DoubleBuilder,
        Int32Builder,
        Int64Builder,
        ListBuilder,
        NullBuilder,
        ObjectIdBuilder,
        StringBuilder,
    )

    _TYPE_TO_BUILDER_CLS = {
        _BsonArrowTypes.int32: Int32Builder,
        _BsonArrowTypes.int64: Int64Builder,
        _BsonArrowTypes.double: DoubleBuilder,
        _BsonArrowTypes.datetime: DatetimeBuilder,
        _BsonArrowTypes.objectid: ObjectIdBuilder,
        _BsonArrowTypes.decimal128: Decimal128Builder,
        _BsonArrowTypes.string: StringBuilder,
        _BsonArrowTypes.bool: BoolBuilder,
        _BsonArrowTypes.document: DocumentBuilder,
        _BsonArrowTypes.array: ListBuilder,
        _BsonArrowTypes.binary: BinaryBuilder,
        _BsonArrowTypes.code: CodeBuilder,
        _BsonArrowTypes.date32: Date32Builder,
        _BsonArrowTypes.date64: Date64Builder,
        _BsonArrowTypes.null: NullBuilder,
    }

except ImportError:
    pass


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
        _parse_types(str_type_map, builder_map, tzinfo)
        return cls(schema, builder_map)

    def finish(self):
        return self._finish(self.builder_map, self.schema)

    @staticmethod
    def _finish(builder_map, schema):
        to_remove = []
        # Traverse the builder map right to left.
        for key, value in reversed(builder_map.items()):
            field = key.decode("utf-8")
            arr = value.finish()
            if isinstance(value, DocumentBuilder):
                full_names = [f"{field}.{name}" for name in arr]
                arrs = [builder_map[c.encode("utf-8")] for c in full_names]
                builder_map[field] = StructArray.from_arrays(arrs, names=arr)
                to_remove.extend(full_names)
            elif isinstance(value, ListBuilder):
                child = field + "[]"
                to_remove.append(child)
                builder_map[key] = ListArray.from_arrays(arr, builder_map.get(child, []))
            else:
                builder_map[key] = arr

        for field in to_remove:
            key = field.encode("utf-8")
            if key in builder_map:
                del builder_map[key]

        arrays = list(builder_map.values())
        if schema is not None:
            return Table.from_arrays(arrays=arrays, schema=schema.to_arrow())
        return Table.from_arrays(arrays=arrays, names=list(builder_map.keys()))


def _parse_types(str_type_map, builder_map, tzinfo):
    for fname, (ftype, arrow_type) in str_type_map.items():
        builder_cls = _TYPE_TO_BUILDER_CLS[ftype]
        encoded_fname = fname.encode("utf-8")
        # special-case initializing builders for parameterized types
        if builder_cls == DatetimeBuilder:
            if tzinfo is not None and arrow_type.tz is None:
                arrow_type = timestamp(arrow_type.unit, tz=tzinfo)  # noqa: PLW2901
            builder_map[encoded_fname] = DatetimeBuilder(dtype=arrow_type)
        elif builder_cls == DocumentBuilder:
            builder_map[encoded_fname] = DocumentBuilder()
            # construct a sub type map here
            sub_type_map = {}
            for i in range(arrow_type.num_fields):
                field = arrow_type[i]
                sub_name = f"{fname}.{field.name}"
                sub_type_map[sub_name] = field.type
            sub_type_map = _get_internal_typemap(sub_type_map)
            _parse_types(sub_type_map, builder_map, tzinfo)
            continue
        elif builder_cls == ListBuilder:
            builder_map[encoded_fname] = ListBuilder()
            if is_struct(arrow_type.value_type):
                # construct a sub type map here
                sub_type_map = {}
                for i in range(arrow_type.value_type.num_fields):
                    field = arrow_type.value_type[i]
                    sub_name = f"{fname}[].{field.name}"
                    sub_type_map[sub_name] = field.type
                sub_type_map = _get_internal_typemap(sub_type_map)
                _parse_types(sub_type_map, builder_map, tzinfo)
                continue
        elif builder_cls == BinaryBuilder:
            subtype = arrow_type.subtype
            builder_map[encoded_fname] = BinaryBuilder(subtype)
        else:
            builder_map[encoded_fname] = builder_cls()
