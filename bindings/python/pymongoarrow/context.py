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
from pyarrow import ListArray, StructArray, Table

from pymongoarrow.types import _BsonArrowTypes, _get_internal_typemap


class PyMongoArrowContext:
    """A context for converting BSON-formatted data to an Arrow Table."""

    def __init__(self, schema, codec_options=None):
        """Initialize the context.

        :Parameters:
          - `schema`: Instance of :class:`~pymongoarrow.schema.Schema`.
          - `builder_map`: Mapping of utf-8-encoded field names to
            :class:`~pymongoarrow.builders._BuilderBase` instances.
        """
        self.schema = schema
        if self.schema is None and codec_options is not None:
            self.tzinfo = codec_options.tzinfo
        else:
            self.tzinfo = None
        schema_map = {}
        if self.schema is not None:
            str_type_map = _get_internal_typemap(schema.typemap)
            _parse_types(str_type_map, schema_map, self.tzinfo)

        # Delayed import to prevent import errors for unbuilt library.
        from pymongoarrow.lib import BuilderManager

        self.manager = BuilderManager(schema_map, self.schema is not None, self.tzinfo)
        self.schema_map = schema_map

    def process_bson_stream(self, stream):
        self.manager.process_bson_stream(stream, len(stream))

    def finish(self):
        array_map = _parse_builder_map(self.manager.finish())
        arrays = list(array_map.values())
        if self.schema is not None:
            return Table.from_arrays(arrays=arrays, schema=self.schema.to_arrow())
        return Table.from_arrays(arrays=arrays, names=list(array_map.keys()))


def _parse_builder_map(builder_map):
    # Handle nested builders.
    to_remove = []
    # Traverse the builder map right to left.
    for key, value in reversed(builder_map.items()):
        if value.type_marker == _BsonArrowTypes.document.value:
            names = []
            full_names = []
            for candidate in list(builder_map):
                if candidate.startswith(key + "."):
                    name = candidate[len(key) + 1 :]
                    if "." in name or "[" in name:
                        continue
                    names.append(name)
                    full_names.append(candidate)
            arrs = [builder_map[c] for c in full_names]
            builder_map[key] = StructArray.from_arrays(arrs, names=names)
            to_remove.extend(full_names)
        elif value.type_marker == _BsonArrowTypes.array.value:
            child_name = key + "[]"
            to_remove.append(child_name)
            child = builder_map[child_name]
            builder_map[key] = ListArray.from_arrays(value.finish(), child)
        else:
            builder_map[key] = value.finish()

    for key in to_remove:
        if key in builder_map:
            del builder_map[key]

    return builder_map


def _parse_types(str_type_map, schema_map, tzinfo):
    for fname, (ftype, arrow_type) in str_type_map.items():
        schema_map[fname] = ftype, arrow_type

        # special-case nested builders
        if ftype == _BsonArrowTypes.document.value:
            # construct a sub type map here
            sub_type_map = {}
            for i in range(arrow_type.num_fields):
                field = arrow_type[i]
                sub_name = f"{fname}.{field.name}"
                sub_type_map[sub_name] = field.type
            sub_type_map = _get_internal_typemap(sub_type_map)
            _parse_types(sub_type_map, schema_map, tzinfo)
        elif ftype == _BsonArrowTypes.array.value:
            sub_type_map = {}
            sub_name = f"{fname}[]"
            sub_value_type = arrow_type.value_type
            sub_type_map[sub_name] = sub_value_type
            sub_type_map = _get_internal_typemap(sub_type_map)
            _parse_types(sub_type_map, schema_map, tzinfo)
