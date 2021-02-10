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
from pyarrow import Table
from pymongoarrow.lib import Int32Builder, Int64Builder, DoubleBuilder, DatetimeBuilder
from pymongoarrow.types import _get_internal_typemap, _BsonArrowTypes


_TYPE_TO_BUILDER_CLS = {
    _BsonArrowTypes.int32: Int32Builder,
    _BsonArrowTypes.int64: Int64Builder,
    _BsonArrowTypes.double: DoubleBuilder,
    _BsonArrowTypes.datetime: DatetimeBuilder
}


class PyMongoArrowContext:
    def __init__(self, schema, builder_map, type_map):
        self.schema = schema
        self.builder_map = builder_map
        self.type_map = type_map

    @classmethod
    def from_schema(cls, schema):
        builder_map = {}
        type_map = {}
        str_type_map = _get_internal_typemap(schema.typemap)
        for fname, ftype in str_type_map.items():
            builder_cls = _TYPE_TO_BUILDER_CLS[ftype]
            encoded_fname = fname.encode('utf-8')
            builder_map[encoded_fname] = builder_cls()   # TODO: support passing parameterized types to Builders (e.g. datetime)
            type_map[encoded_fname] = ftype
        return cls(schema, builder_map, type_map)

    def __contains__(self, item):
        if isinstance(item, str):
            return item in self.schema
        elif isinstance(item, bytes):
            return item in self.builder_map
        else:
            raise TypeError('context keys are either str or bytes type')

    def finish(self):
        arrays = []
        names = []
        for fname, builder in self.builder_map.items():
            arrays.append(builder.finish())
            names.append(fname.decode('utf-8'))
        return Table.from_arrays(arrays=arrays, names=names)
