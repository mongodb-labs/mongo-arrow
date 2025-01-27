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

# Cython compiler directives
# distutils: language=c++
# cython: language_level=3

# Stdlib imports
import sys

# Python imports
import bson
import numpy as np
from pyarrow import timestamp, default_memory_pool

from pymongoarrow.errors import InvalidBSON
from pymongoarrow.types import ObjectIdType, Decimal128Type as Decimal128Type_, BinaryType, CodeType

# Cython imports
from cpython cimport object
from libcpp cimport bool as cbool
from libc.math cimport isnan
from libcpp.string cimport string as cstring
from libc.string cimport memcpy
from libcpp cimport nullptr
from pyarrow.lib cimport *
from pymongoarrow.libarrow cimport *
from pymongoarrow.libbson cimport *

# Placeholder numbers for the date types.
# Keep in sync with _BsonArrowTypes in types.py.
cdef uint8_t ARROW_TYPE_DATE32 = 100
cdef uint8_t ARROW_TYPE_DATE64 = 101
cdef uint8_t ARROW_TYPE_NULL = 102

# libbson version
libbson_version = bson_get_version().decode('utf-8')


cdef const bson_t* bson_reader_read_safe(bson_reader_t* stream_reader) except? NULL:
    cdef cbool reached_eof = False
    cdef const bson_t* doc = bson_reader_read(stream_reader, &reached_eof)
    if doc == NULL and not reached_eof:
        raise InvalidBSON("Could not read BSON document stream")
    return doc


cdef class BuilderManager:
    cdef:
        dict builder_map
        dict parent_types
        dict parent_names
        uint64_t count
        bint has_schema
        object tzinfo
        object pool

    def __cinit__(self, dict schema_map, bint has_schema, object tzinfo):
        self.has_schema = has_schema
        self.tzinfo = tzinfo
        self.count = 0
        self.builder_map = {}
        self.parent_names = {}
        self.parent_types = {}
        self.pool = default_memory_pool()
        # Unpack the schema map.
        for fname, (ftype, arrow_type) in schema_map.items():
            name = fname.encode('utf-8')
            # special-case initializing builders for parameterized types
            if ftype == BSON_TYPE_DATE_TIME:
                if tzinfo is not None and arrow_type.tz is None:
                    arrow_type = timestamp(arrow_type.unit, tz=tzinfo)  # noqa: PLW2901
                self.builder_map[name] = DatetimeBuilder(dtype=arrow_type, memory_pool=self.pool)
            elif ftype == BSON_TYPE_BINARY:
                self.builder_map[name] = BinaryBuilder(arrow_type.subtype, memory_pool=self.pool)
            else:
                # We only use the doc_iter for binary arrays, which are handled already.
                self.get_builder(name, ftype, <bson_iter_t *>nullptr)

    cdef _ArrayBuilderBase get_builder(self, cstring key, bson_type_t value_t, bson_iter_t * doc_iter):
        cdef _ArrayBuilderBase builder = None
        cdef bson_subtype_t subtype
        cdef const uint8_t *val_buf = NULL
        cdef uint32_t val_buf_len = 0

        # Mark a null key as missing until we find it.
        if value_t == BSON_TYPE_NULL:
            self.builder_map[key] = None
            return

        if builder is not None:
            return builder

        # Handle the builders.
        if value_t == BSON_TYPE_DATE_TIME:
            if self.tzinfo is not None:
                arrow_type = timestamp('ms', tz=self.tzinfo)
                builder = DatetimeBuilder(dtype=arrow_type, memory_pool=self.pool)
            else:
                builder = DatetimeBuilder(memory_pool=self.pool)
        elif value_t == BSON_TYPE_DOCUMENT:
            builder = DocumentBuilder()
        elif value_t == BSON_TYPE_ARRAY:
            builder = ListBuilder(memory_pool=self.pool)
        elif value_t == BSON_TYPE_BINARY:
            if doc_iter == NULL:
                raise ValueError('Did not pass a doc_iter!')
            bson_iter_binary (doc_iter, &subtype,
                &val_buf_len, &val_buf)
            builder = BinaryBuilder(subtype, memory_pool=self.pool)
        elif value_t == ARROW_TYPE_DATE32:
            builder = Date32Builder(memory_pool=self.pool)
        elif value_t == ARROW_TYPE_DATE64:
            builder = Date64Builder(memory_pool=self.pool)
        elif value_t == BSON_TYPE_INT32:
            builder = Int32Builder(memory_pool=self.pool)
        elif value_t == BSON_TYPE_INT64:
            builder = Int64Builder(memory_pool=self.pool)
        elif value_t == BSON_TYPE_DOUBLE:
            builder = DoubleBuilder(memory_pool=self.pool)
        elif value_t == BSON_TYPE_OID:
            builder = ObjectIdBuilder(memory_pool=self.pool)
        elif value_t == BSON_TYPE_UTF8:
            builder = StringBuilder(memory_pool=self.pool)
        elif value_t == BSON_TYPE_BOOL:
            builder = BoolBuilder(memory_pool=self.pool)
        elif value_t == BSON_TYPE_DECIMAL128:
            builder = Decimal128Builder(memory_pool=self.pool)
        elif value_t == BSON_TYPE_CODE:
            builder = CodeBuilder(memory_pool=self.pool)

        self.builder_map[key] = builder
        return builder

    cdef void parse_document(self, bson_iter_t * doc_iter, cstring base_key, uint8_t parent_type) except *:
        cdef bson_type_t value_t
        cdef cstring key
        cdef cstring full_key
        cdef bson_iter_t child_iter
        cdef uint64_t count = self.count
        cdef _ArrayBuilderBase builder = None
        cdef _ArrayBuilderBase parent_builder = None

        while bson_iter_next(doc_iter):
            # Get the key and and value.
            key = bson_iter_key(doc_iter)
            value_t = bson_iter_type(doc_iter)

            # Get the appropriate full key.
            if parent_type == BSON_TYPE_ARRAY:
                full_key = base_key
                full_key.append(b"[]")
                self.parent_types[full_key] = BSON_TYPE_ARRAY

            elif parent_type == BSON_TYPE_DOCUMENT:
                full_key = base_key
                full_key.append(b".")
                full_key.append(key)
                (<DocumentBuilder>self.builder_map[base_key]).add_field(key)
                self.parent_types[full_key] = BSON_TYPE_DOCUMENT
                self.parent_names[full_key] = base_key

            else:
                full_key = key

            # Get the builder.
            builder = <_ArrayBuilderBase>self.builder_map.get(full_key, None)
            if builder is None and not self.has_schema:
                builder = self.get_builder(full_key, value_t, doc_iter)
            if builder is None:
                continue

            # Append nulls to catch up.
            # For list children, the nulls are stored in the parent.
            if parent_type != BSON_TYPE_ARRAY:
                # For document children, catch up with the parent doc.
                # Root fields will use the base document count
                if parent_type == BSON_TYPE_DOCUMENT:
                    parent_builder = <_ArrayBuilderBase>self.builder_map.get(base_key, None)
                    count = parent_builder.length() - 1
                if count > builder.length():
                    status = builder.append_nulls_raw(count - builder.length())
                    if not status.ok():
                        raise ValueError("Failed to append nulls to", full_key.decode('utf8'))

            # Append the next value.
            status = builder.append_raw(doc_iter, value_t)
            if not status.ok():
                raise ValueError("Could not append raw value to", full_key.decode('utf8'))

            # Recurse into documents.
            if value_t == BSON_TYPE_DOCUMENT and builder.type_marker == BSON_TYPE_DOCUMENT:
                bson_iter_recurse(doc_iter, &child_iter)
                self.parse_document(&child_iter, full_key, BSON_TYPE_DOCUMENT)

            # Recurse into arrays.
            if value_t == BSON_TYPE_ARRAY and builder.type_marker == BSON_TYPE_ARRAY:
                bson_iter_recurse(doc_iter, &child_iter)
                self.parse_document(&child_iter, full_key, BSON_TYPE_ARRAY)

            # If we're a list element, increment the offset counter.
            if parent_type == BSON_TYPE_ARRAY:
                (<ListBuilder>self.builder_map[base_key]).append_count()

    cpdef void process_bson_stream(self, const uint8_t* bson_stream, size_t length):
        """Process a bson byte stream."""
        cdef bson_reader_t* stream_reader = bson_reader_new_from_data(bson_stream, length)
        cdef const bson_t * doc = NULL
        cdef bson_iter_t doc_iter
        try:
            while True:
                doc = bson_reader_read_safe(stream_reader)
                if doc == NULL:
                    break
                if not bson_iter_init(&doc_iter, doc):
                    raise InvalidBSON("Could not read BSON document")
                self.parse_document(&doc_iter, b"", 0)
                self.count += 1
        finally:
                bson_reader_destroy(stream_reader)

    cpdef finish(self):
        """Finish appending to the builders."""
        cdef dict return_map = {}
        cdef bytes key
        cdef str field
        cdef uint64_t count
        cdef CStatus status
        cdef _ArrayBuilderBase builder
        cdef _ArrayBuilderBase parent_builder

        # Move the builders to a new dict with string keys.
        for key, builder in self.builder_map.items():
            return_map[key.decode('utf-8')] = builder

        # Insert null fields.
        for field in list(return_map):
            if return_map[field] is None:
                return_map[field] = NullBuilder(memory_pool=self.pool)

        # Pad fields as needed.
        for field, builder in return_map.items():
            # For list children, the nulls are stored in the parent.
            key = field.encode('utf-8')
            parent_type = self.parent_types.get(key, None)
            # Check if the item was in our schema but never seen, and should have a parent.
            if parent_type is None and "." in field:
                parent_key, _, _ = field.rpartition('.')
                self.parent_names[key] = parent_key.encode('utf-8')
                parent_type = BSON_TYPE_DOCUMENT
            # Add nulls according to parent type.
            if parent_type == BSON_TYPE_ARRAY:
                continue
            if parent_type == BSON_TYPE_DOCUMENT:
                parent_builder = self.builder_map[self.parent_names[key]]
                count = parent_builder.length()
            else:
                count = self.count
            if builder.length() < count:
                status = builder.append_nulls_raw(count - builder.length())
                if not status.ok():
                    raise ValueError("Failed to append nulls to", field)

        return return_map


cdef class _ArrayBuilderBase:
    cdef:
        public uint8_t type_marker

    def append_values(self, values):
        for value in values:
            if value is None or value is np.nan:
                self.append_null()
            else:
                self.append(value)

    def append(self, value):
        """Interface to append a python value to the builder.
        """
        cdef bson_reader_t* stream_reader = NULL
        cdef const bson_t * doc = NULL
        cdef bson_iter_t doc_iter

        data = bson.encode(dict(data=value))
        stream_reader = bson_reader_new_from_data(data, len(data))
        doc = bson_reader_read_safe(stream_reader)
        if doc == NULL:
            raise ValueError("Could not append", value)
        if not bson_iter_init(&doc_iter, doc):
            raise InvalidBSON("Could not read BSON document")
        while bson_iter_next(&doc_iter):
            bson_iter_key(&doc_iter)
            value_t = bson_iter_type(&doc_iter)
            status = self.append_raw(&doc_iter, value_t)
            if not status.ok():
                raise ValueError("Could not append raw value of type", value_t)

    cdef CStatus append_raw(self, bson_iter_t * doc_iter, bson_type_t value_t):
        pass

    cdef shared_ptr[CArrayBuilder] get_builder(self):
        pass

    def __len__(self):
        return self.length()

    cpdef append_null(self):
        cdef CStatus status = self.append_null_raw()
        if not status.ok():
            raise ValueError("Could not append null value")

    cpdef void append_nulls(self, uint64_t count):
        for _ in range(count):
            self.append_null()

    cdef CStatus append_null_raw(self):
        return self.get_builder().get().AppendNull()

    cdef CStatus append_nulls_raw(self, uint64_t count):
        cdef CStatus status
        for _ in range(count):
            status = self.append_null_raw()
            if not status.ok():
                return status

    cpdef uint64_t length(self):
        return self.get_builder().get().length()

    def finish(self):
        cdef shared_ptr[CArray] out
        cdef CStatus status
        cdef shared_ptr[CArrayBuilder] builder = self.get_builder()
        with nogil:
            status = builder.get().Finish(&out)
        if not status.ok():
            raise ValueError("Failed to convert value to array")
        return pyarrow_wrap_array(out)


cdef class StringBuilder(_ArrayBuilderBase):
    cdef:
        shared_ptr[CStringBuilder] builder

    def __cinit__(self, MemoryPool memory_pool=None):
        cdef CMemoryPool* pool = maybe_unbox_memory_pool(memory_pool)
        self.builder.reset(new CStringBuilder(pool))
        self.type_marker = BSON_TYPE_UTF8

    cdef CStatus append_raw(self, bson_iter_t * doc_iter, bson_type_t value_t):
        cdef const char* value
        cdef uint32_t str_len
        if value_t == BSON_TYPE_UTF8:
            value = bson_iter_utf8(doc_iter, &str_len)
            return self.builder.get().Append(value, str_len)
        return self.builder.get().AppendNull()

    cdef shared_ptr[CArrayBuilder] get_builder(self):
        return <shared_ptr[CArrayBuilder]>self.builder


cdef class CodeBuilder(StringBuilder):
    def __cinit__(self, MemoryPool memory_pool=None):
        cdef CMemoryPool* pool = maybe_unbox_memory_pool(memory_pool)
        self.builder.reset(new CStringBuilder(pool))
        self.type_marker = BSON_TYPE_CODE

    cdef CStatus append_raw(self, bson_iter_t * doc_iter, bson_type_t value_t):
        cdef const char * bson_str
        cdef uint32_t str_len
        if value_t == BSON_TYPE_CODE:
            bson_str = bson_iter_code(doc_iter, &str_len)
            return self.builder.get().Append(bson_str, str_len)
        return self.builder.get().AppendNull()

    cdef shared_ptr[CArrayBuilder] get_builder(self):
        return <shared_ptr[CArrayBuilder]>self.builder

    def finish(self):
        return super().finish().cast(CodeType())


cdef class ObjectIdBuilder(_ArrayBuilderBase):
    cdef shared_ptr[CFixedSizeBinaryBuilder] builder

    def __cinit__(self, MemoryPool memory_pool=None):
        cdef shared_ptr[CDataType] dtype = fixed_size_binary(12)
        cdef CMemoryPool* pool = maybe_unbox_memory_pool(memory_pool)
        self.builder.reset(new CFixedSizeBinaryBuilder(dtype, pool))
        self.type_marker = BSON_TYPE_OID

    cdef CStatus append_raw(self, bson_iter_t * doc_iter, bson_type_t value_t):
        if value_t == BSON_TYPE_OID:
            return self.builder.get().Append(bson_iter_oid(doc_iter).bytes)
        return self.builder.get().AppendNull()

    cdef shared_ptr[CArrayBuilder] get_builder(self):
        return <shared_ptr[CArrayBuilder]>self.builder

    def finish(self):
        return super().finish().cast(ObjectIdType())


cdef class Int32Builder(_ArrayBuilderBase):
    cdef shared_ptr[CInt32Builder] builder

    def __cinit__(self, MemoryPool memory_pool=None):
        cdef CMemoryPool* pool = maybe_unbox_memory_pool(memory_pool)
        self.builder.reset(new CInt32Builder(pool))
        self.type_marker = BSON_TYPE_INT32

    cdef CStatus append_raw(self, bson_iter_t * doc_iter, bson_type_t value_t) except *:
        cdef double dvalue
        cdef int64_t ivalue

        if (value_t == BSON_TYPE_INT32 or value_t == BSON_TYPE_BOOL or value_t == BSON_TYPE_INT64):
            # Check for overflow errors.
            ivalue = bson_iter_as_int64(doc_iter)
            if ivalue > INT_MAX or ivalue < INT_MIN:
                raise OverflowError("Overflowed Int32 value")
            return self.builder.get().Append(ivalue)
        if value_t == BSON_TYPE_DOUBLE:
            # Treat nan as null.
            dvalue = bson_iter_as_double(doc_iter)
            if isnan(dvalue):
               return self.builder.get().AppendNull()
            # Check for overflow errors.
            ivalue = bson_iter_as_int64(doc_iter)
            if ivalue > INT_MAX or ivalue < INT_MIN:
                raise OverflowError("Overflowed Int32 value")
            return self.builder.get().Append(ivalue)
        return self.builder.get().AppendNull()

    cdef shared_ptr[CArrayBuilder] get_builder(self):
        return <shared_ptr[CArrayBuilder]>self.builder


cdef class Int64Builder(_ArrayBuilderBase):
    cdef shared_ptr[CInt64Builder] builder

    def __cinit__(self, MemoryPool memory_pool=None):
        cdef CMemoryPool* pool = maybe_unbox_memory_pool(memory_pool)
        self.builder.reset(new CInt64Builder(pool))
        self.type_marker = BSON_TYPE_INT64

    cdef CStatus append_raw(self, bson_iter_t * doc_iter, bson_type_t value_t):
        cdef double dvalue

        if (value_t == BSON_TYPE_INT64 or
                value_t == BSON_TYPE_BOOL or
                value_t == BSON_TYPE_INT32):
            return self.builder.get().Append(bson_iter_as_int64(doc_iter))
        if value_t == BSON_TYPE_DOUBLE:
            # Treat nan as null.
            dvalue = bson_iter_as_double(doc_iter)
            if isnan(dvalue):
                return self.builder.get().AppendNull()
            return self.builder.get().Append(bson_iter_as_int64(doc_iter))
        return self.builder.get().AppendNull()

    cdef shared_ptr[CArrayBuilder] get_builder(self):
        return <shared_ptr[CArrayBuilder]>self.builder


cdef class DoubleBuilder(_ArrayBuilderBase):
    cdef shared_ptr[CDoubleBuilder] builder

    def __cinit__(self, MemoryPool memory_pool=None):
        cdef CMemoryPool* pool = maybe_unbox_memory_pool(memory_pool)
        self.builder.reset(new CDoubleBuilder(pool))
        self.type_marker = BSON_TYPE_DOUBLE

    cdef CStatus append_raw(self, bson_iter_t * doc_iter, bson_type_t value_t):
        if (value_t == BSON_TYPE_DOUBLE or
                    value_t == BSON_TYPE_BOOL or
                    value_t == BSON_TYPE_INT32 or
                    value_t == BSON_TYPE_INT64):
            return self.builder.get().Append(bson_iter_as_double(doc_iter))
        return self.builder.get().AppendNull()

    cdef shared_ptr[CArrayBuilder] get_builder(self):
        return <shared_ptr[CArrayBuilder]>self.builder


cdef class DatetimeBuilder(_ArrayBuilderBase):
    cdef:
        TimestampType dtype
        shared_ptr[CTimestampBuilder] builder

    def __cinit__(self, TimestampType dtype=timestamp('ms'),
                  MemoryPool memory_pool=None):
        cdef CMemoryPool* pool = maybe_unbox_memory_pool(memory_pool)
        if dtype.unit != 'ms':
            raise TypeError("PyMongoArrow only supports millisecond "
                            "temporal resolution compatible with MongoDB's "
                            "UTC datetime type.")
        self.dtype = dtype
        self.builder.reset(new CTimestampBuilder(
            pyarrow_unwrap_data_type(self.dtype), pool))
        self.type_marker = BSON_TYPE_DATE_TIME

    @property
    def unit(self):
        return self.dtype

    cdef CStatus append_raw(self, bson_iter_t * doc_iter, bson_type_t value_t):
        if value_t == BSON_TYPE_DATE_TIME:
            return self.builder.get().Append(bson_iter_date_time(doc_iter))
        return self.builder.get().AppendNull()

    cdef shared_ptr[CArrayBuilder] get_builder(self):
        return <shared_ptr[CArrayBuilder]>self.builder

cdef class Date64Builder(_ArrayBuilderBase):
    cdef:
        DataType dtype
        shared_ptr[CDate64Builder] builder

    def __cinit__(self, MemoryPool memory_pool=None):
        cdef CMemoryPool* pool = maybe_unbox_memory_pool(memory_pool)
        self.builder.reset(new CDate64Builder(pool))
        self.type_marker = ARROW_TYPE_DATE64

    cdef CStatus append_raw(self, bson_iter_t * doc_iter, bson_type_t value_t):
        if value_t == BSON_TYPE_DATE_TIME:
            return self.builder.get().Append(bson_iter_date_time(doc_iter))
        return self.builder.get().AppendNull()

    @property
    def unit(self):
        return self.dtype

    cdef shared_ptr[CArrayBuilder] get_builder(self):
        return <shared_ptr[CArrayBuilder]>self.builder


cdef class Date32Builder(_ArrayBuilderBase):
    cdef:
        DataType dtype
        shared_ptr[CDate32Builder] builder

    def __cinit__(self, MemoryPool memory_pool=None):
        cdef CMemoryPool* pool = maybe_unbox_memory_pool(memory_pool)
        self.builder.reset(new CDate32Builder(pool))
        self.type_marker = ARROW_TYPE_DATE32

    cdef CStatus append_raw(self, bson_iter_t * doc_iter, bson_type_t value_t):
        cdef int64_t value
        cdef int32_t seconds_val

        if value_t == BSON_TYPE_DATE_TIME:
            value = bson_iter_date_time(doc_iter)
            # Convert from milliseconds to days (1000*60*60*24)
            seconds_val = value // 86400000
            return self.builder.get().Append(seconds_val)
        return self.builder.get().AppendNull()

    @property
    def unit(self):
        return self.dtype

    cdef shared_ptr[CArrayBuilder] get_builder(self):
        return <shared_ptr[CArrayBuilder]>self.builder

cdef class NullBuilder(_ArrayBuilderBase):
    cdef shared_ptr[CArrayBuilder] builder

    def __cinit__(self, MemoryPool memory_pool=None):
        cdef CMemoryPool* pool = maybe_unbox_memory_pool(memory_pool)
        self.builder.reset(new CNullBuilder(pool))
        self.type_marker = ARROW_TYPE_NULL

    cdef CStatus append_raw(self, bson_iter_t * doc_iter, bson_type_t value_t):
        return self.builder.get().AppendNull()

    cdef shared_ptr[CArrayBuilder] get_builder(self):
        return <shared_ptr[CArrayBuilder]>self.builder


cdef class BoolBuilder(_ArrayBuilderBase):
    cdef shared_ptr[CBooleanBuilder] builder

    def __cinit__(self, MemoryPool memory_pool=None):
        cdef CMemoryPool* pool = maybe_unbox_memory_pool(memory_pool)
        self.builder.reset(new CBooleanBuilder(pool))
        self.type_marker = BSON_TYPE_BOOL

    cdef CStatus append_raw(self, bson_iter_t * doc_iter, bson_type_t value_t):
        if value_t == BSON_TYPE_BOOL:
            return self.builder.get().Append(bson_iter_bool(doc_iter))
        return self.builder.get().AppendNull()

    cdef shared_ptr[CArrayBuilder] get_builder(self):
        return <shared_ptr[CArrayBuilder]>self.builder

cdef class Decimal128Builder(_ArrayBuilderBase):
    cdef shared_ptr[CFixedSizeBinaryBuilder] builder
    cdef uint8_t supported

    def __cinit__(self, MemoryPool memory_pool=None):
        cdef shared_ptr[CDataType] dtype = fixed_size_binary(16)
        cdef CMemoryPool* pool = maybe_unbox_memory_pool(memory_pool)
        self.builder.reset(new CFixedSizeBinaryBuilder(dtype, pool))
        self.type_marker = BSON_TYPE_DECIMAL128
        if sys.byteorder == 'little':
            self.supported = 1
        else:
            self.supported = 0

    cdef CStatus append_raw(self, bson_iter_t * doc_iter, bson_type_t value_t):
        cdef uint8_t dec128_buf[16]
        cdef bson_decimal128_t dec128

        if self.supported == 0:
            # We do not support big-endian systems.
            return self.builder.get().AppendNull()

        if value_t == BSON_TYPE_DECIMAL128:
            bson_iter_decimal128(doc_iter, &dec128)
            memcpy(dec128_buf, &dec128.low, 8);
            memcpy(dec128_buf + 8, &dec128.high, 8)
            return self.builder.get().Append(dec128_buf)
        return self.builder.get().AppendNull()

    cdef shared_ptr[CArrayBuilder] get_builder(self):
        return <shared_ptr[CArrayBuilder]>self.builder

    def finish(self):
        return super().finish().cast(Decimal128Type_())


cdef class BinaryBuilder(_ArrayBuilderBase):
    cdef:
        uint8_t _subtype
        shared_ptr[CStringBuilder] builder

    def __cinit__(self, uint8_t subtype, MemoryPool memory_pool=None):
        cdef CMemoryPool* pool = maybe_unbox_memory_pool(memory_pool)
        self._subtype = subtype
        self.builder.reset(new CStringBuilder(pool))
        self.type_marker = BSON_TYPE_BINARY

    @property
    def subtype(self):
        return self._subtype

    cdef CStatus append_raw(self, bson_iter_t * doc_iter, bson_type_t value_t):
        cdef const char * val_buf
        cdef uint32_t val_buf_len
        cdef bson_subtype_t subtype

        if value_t == BSON_TYPE_BINARY:
            bson_iter_binary(doc_iter, &subtype, &val_buf_len, <const uint8_t **>&val_buf)
            if subtype != self._subtype:
                return self.builder.get().AppendNull()
            return self.builder.get().Append(val_buf, val_buf_len)
        return self.builder.get().AppendNull()

    cdef shared_ptr[CArrayBuilder] get_builder(self):
        return <shared_ptr[CArrayBuilder]>self.builder

    def finish(self):
        return super().finish().cast(BinaryType(self._subtype))


cdef class DocumentBuilder(_ArrayBuilderBase):
    """The document builder stores a map of field names that can be retrieved as a set."""
    cdef:
        dict field_map
        int64_t count

    def __cinit__(self):
        self.type_marker = BSON_TYPE_DOCUMENT
        self.field_map = dict()

    cdef CStatus append_raw(self, bson_iter_t * doc_iter, bson_type_t value_t):
        self.count += 1
        return CStatus_OK()

    cpdef uint64_t length(self):
        return self.count

    cdef CStatus append_null_raw(self):
        self.count += 1
        return CStatus_OK()

    cpdef void add_field(self, cstring field_name):
        self.field_map[field_name] = 1

    def finish(self):
        # Fields must be in order if we were given a schema.
        return list(f.decode('utf-8') for f in self.field_map)


cdef class ListBuilder(_ArrayBuilderBase):
    """The list builder stores an int32 list of offsets and a counter with the current value."""
    cdef:
        int64_t count
        shared_ptr[CInt32Builder] builder

    def __cinit__(self, MemoryPool memory_pool=None):
        cdef CMemoryPool* pool = maybe_unbox_memory_pool(memory_pool)
        self.builder.reset(new CInt32Builder(pool))
        self.count = 0
        self.type_marker = BSON_TYPE_ARRAY

    cdef CStatus append_raw(self, bson_iter_t * doc_iter, bson_type_t value_t):
        if value_t == BSON_TYPE_NULL:
            return self.append_null_raw()
        return self.builder.get().Append(self.count)

    cpdef void append_count(self):
        self.count += 1

    cdef CStatus append_null_raw(self):
        return self.builder.get().AppendNull()

    cdef shared_ptr[CArrayBuilder] get_builder(self):
        return <shared_ptr[CArrayBuilder]>self.builder

    def finish(self):
        self.builder.get().Append(self.count)
        return super().finish()
