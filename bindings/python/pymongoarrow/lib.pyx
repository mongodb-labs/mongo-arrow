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
from pyarrow import timestamp

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
        uint32_t count
        bint has_schema
        object tzinfo

    def __cinit__(self, dict schema_map, bint has_schema, object tzinfo):
        self.has_schema = has_schema
        self.tzinfo = tzinfo
        self.count = 0
        self.builder_map = builder_map = {}
        # Unpack the schema map.
        for fname, (ftype, arrow_type) in schema_map.items():
            encoded_fname = fname.encode("utf-8")
            # special-case initializing builders for parameterized types
            if ftype == BSON_TYPE_DATE_TIME:
                if tzinfo is not None and arrow_type.tz is None:
                    arrow_type = timestamp(arrow_type.unit, tz=tzinfo)  # noqa: PLW2901
                builder_map[encoded_fname] = DatetimeBuilder(dtype=arrow_type)
            elif ftype == BSON_TYPE_BINARY:
                subtype = arrow_type.subtype
                builder_map[encoded_fname] = BinaryBuilder(subtype)
            else:
                self.get_builder(encoded_fname, ftype, <bson_iter_t *>nullptr)

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
                builder = DatetimeBuilder(dtype=arrow_type)
            else:
                builder = DatetimeBuilder()
        elif value_t == BSON_TYPE_DOCUMENT:
            builder = DocumentBuilder()
        elif value_t == BSON_TYPE_ARRAY:
            builder = ListBuilder()
        elif value_t == BSON_TYPE_BINARY:
            bson_iter_binary (doc_iter, &subtype,
                &val_buf_len, &val_buf)
            builder = BinaryBuilder(subtype)
        elif value_t == ARROW_TYPE_DATE32:
            builder = Date32Builder()
        elif value_t == ARROW_TYPE_DATE64:
            builder = Date64Builder()
        elif value_t == BSON_TYPE_INT32:
            builder = Int32Builder()
        elif value_t == BSON_TYPE_INT64:
            builder = Int64Builder()
        elif value_t == BSON_TYPE_DOUBLE:
            builder = DoubleBuilder()
        elif value_t == BSON_TYPE_OID:
            builder = ObjectIdBuilder()
        elif value_t == BSON_TYPE_UTF8:
            builder = StringBuilder()
        elif value_t == BSON_TYPE_BOOL:
            builder = BoolBuilder()
        elif value_t == BSON_TYPE_DECIMAL128:
            builder = Decimal128Builder()
        elif value_t == BSON_TYPE_CODE:
            builder = CodeBuilder()

        self.builder_map[key] = builder
        return builder

    cdef uint8_t parse_document(self, bson_iter_t * doc_iter, cstring base_key, uint8_t parent_type) except *:
        cdef bson_type_t value_t
        cdef cstring key
        cdef cstring full_key
        cdef bson_iter_t child_iter
        cdef uint32_t count = self.count
        cdef _ArrayBuilderBase builder = None

        while bson_iter_next(doc_iter):
            # Get the key and and value.
            key = bson_iter_key(doc_iter)
            value_t = bson_iter_type(doc_iter)
            print('handling', key, value_t)

            # Get the appropriate full key.
            if parent_type == BSON_TYPE_ARRAY:
                full_key = base_key
                full_key.append(b"[]")

            elif parent_type == BSON_TYPE_DOCUMENT:
                full_key = base_key
                full_key.append(b".")
                full_key.append(key)
                (<DocumentBuilder>self.builder_map[base_key]).add_field(key)

            else:
                full_key = key

            # Get the builder.
            builder = <_ArrayBuilderBase>self.builder_map.get(full_key, None)
            if builder is None and not self.has_schema:
                builder = self.get_builder(full_key, value_t, doc_iter)
            if builder is None:
                continue

            # Append nulls to catch up.
            # For lists, the nulls are stored in the parent.
            if parent_type != BSON_TYPE_ARRAY:
                if count > builder.length():
                    builder.append_nulls(count - builder.length())

            # Append the next value.
            builder.append_raw(doc_iter, value_t)

            # Recurse into documents.
            if value_t == BSON_TYPE_DOCUMENT:
                bson_iter_recurse(doc_iter, &child_iter)
                self.parse_document(&child_iter, full_key, BSON_TYPE_DOCUMENT)

            # Recurse into arrays.
            if value_t == BSON_TYPE_ARRAY:
                bson_iter_recurse(doc_iter, &child_iter)
                self.parse_document(&child_iter, full_key, BSON_TYPE_ARRAY)

            # If we're a list element, increment the offset counter.
            if parent_type == BSON_TYPE_ARRAY:
                (<ListBuilder>self.builder_map[base_key]).append_count()

            # Update our count.
            if builder.length() > self.count:
                self.count = builder.length()

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
        finally:
                bson_reader_destroy(stream_reader)

    cpdef finish(self):
        """Finish building the arrays."""
        cdef dict builder_map = self.builder_map
        cdef dict array_map = {}
        cdef bytes key
        cdef str field
        cdef _ArrayBuilderBase value

        # Insert null fields.
        for key in list(builder_map):
            if builder_map[key] is None:
                builder_map[key] = NullBuilder(self.count)

        # Pad fields as needed.
        for key, value in builder_map.items():
            field = key.decode("utf-8")

            # If it isn't a list item, append nulls as needed.
            # For lists, the nulls are stored in the parent.
            if not field.endswith('[]'):
                if value.length() < self.count:
                    value.append_nulls(self.count - value.length())

            array_map[field] = value.finish()
        return array_map


cdef class _ArrayBuilderBase:
    cdef:
        uint8_t type_marker

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
            self.append_raw(&doc_iter, value_t)

    cdef void append_raw(self, bson_iter_t * doc_iter, bson_type_t value_t) except *:
        pass

    cdef shared_ptr[CArrayBuilder] get_builder(self):
        pass

    def __len__(self):
        return self.length()

    cpdef void append_null(self):
        self.get_builder().get().AppendNull()

    cpdef void append_nulls(self, uint32_t count):
        for _ in range(count):
            self.append_null()

    cpdef uint32_t length(self):
        return self.get_builder().get().length()

    def finish(self):
        cdef shared_ptr[CArray] out
        cdef shared_ptr[CArrayBuilder] builder = self.get_builder()
        with nogil:
            builder.get().Finish(&out)
        return pyarrow_wrap_array(out)


cdef class StringBuilder(_ArrayBuilderBase):
    cdef:
        shared_ptr[CStringBuilder] builder

    def __cinit__(self, MemoryPool memory_pool=None):
        cdef CMemoryPool* pool = maybe_unbox_memory_pool(memory_pool)
        self.builder.reset(new CStringBuilder(pool))
        self.type_marker = BSON_TYPE_UTF8

    cdef void append_raw(self, bson_iter_t * doc_iter, bson_type_t value_t) except *:
        cdef const char* value
        cdef uint32_t str_len
        if value_t == BSON_TYPE_UTF8:
            value = bson_iter_utf8(doc_iter, &str_len)
            self.builder.get().Append(value, str_len)
        else:
            self.builder.get().AppendNull()

    cdef shared_ptr[CArrayBuilder] get_builder(self):
        return <shared_ptr[CArrayBuilder]>self.builder


cdef class CodeBuilder(StringBuilder):
    def __cinit__(self, MemoryPool memory_pool=None):
        cdef CMemoryPool* pool = maybe_unbox_memory_pool(memory_pool)
        self.builder.reset(new CStringBuilder(pool))
        self.type_marker = BSON_TYPE_CODE

    cdef void append_raw(self, bson_iter_t * doc_iter, bson_type_t value_t) except *:
        cdef const char * bson_str
        cdef uint32_t str_len
        if value_t == BSON_TYPE_CODE:
            bson_str = bson_iter_code(doc_iter, &str_len)
            self.builder.get().Append(bson_str, str_len)
        else:
            self.builder.get().AppendNull()

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

    cdef void append_raw(self, bson_iter_t * doc_iter, bson_type_t value_t) except *:
        if value_t == BSON_TYPE_OID:
            self.builder.get().Append(bson_iter_oid(doc_iter).bytes)
        else:
            self.builder.get().AppendNull()

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

    cdef void append_raw(self, bson_iter_t * doc_iter, bson_type_t value_t) except *:
        cdef double dvalue

        if (value_t == BSON_TYPE_INT32 or value_t == BSON_TYPE_BOOL or value_t == BSON_TYPE_INT64):
            # The builder will surface overflow errors.
            self.builder.get().Append(<int32_t>bson_iter_as_int64(doc_iter))
        elif value_t == BSON_TYPE_DOUBLE:
            # Treat nan as null.
            dvalue = bson_iter_as_double(doc_iter)
            if isnan(dvalue):
                self.builder.get().AppendNull()
            else:
                # The builder will surface overflow errors.
                self.builder.get().Append(<int32_t>bson_iter_as_int64(doc_iter))
        else:
            self.builder.get().AppendNull()

    cdef shared_ptr[CArrayBuilder] get_builder(self):
        return <shared_ptr[CArrayBuilder]>self.builder


cdef class Int64Builder(_ArrayBuilderBase):
    cdef shared_ptr[CInt64Builder] builder

    def __cinit__(self, MemoryPool memory_pool=None):
        cdef CMemoryPool* pool = maybe_unbox_memory_pool(memory_pool)
        self.builder.reset(new CInt64Builder(pool))
        self.type_marker = BSON_TYPE_INT64

    cdef void append_raw(self, bson_iter_t * doc_iter, bson_type_t value_t) except *:
        cdef double dvalue

        if (value_t == BSON_TYPE_INT64 or
                value_t == BSON_TYPE_BOOL or
                value_t == BSON_TYPE_INT32):
            self.builder.get().Append(bson_iter_as_int64(doc_iter))
        elif value_t == BSON_TYPE_DOUBLE:
            # Treat nan as null.
            dvalue = bson_iter_as_double(doc_iter)
            if isnan(dvalue):
                self.builder.get().AppendNull()
            else:
                self.builder.get().Append(bson_iter_as_int64(doc_iter))
        else:
            self.builder.get().AppendNull()

    cdef shared_ptr[CArrayBuilder] get_builder(self):
        return <shared_ptr[CArrayBuilder]>self.builder


cdef class DoubleBuilder(_ArrayBuilderBase):
    cdef shared_ptr[CDoubleBuilder] builder

    def __cinit__(self, MemoryPool memory_pool=None):
        cdef CMemoryPool* pool = maybe_unbox_memory_pool(memory_pool)
        self.builder.reset(new CDoubleBuilder(pool))
        self.type_marker = BSON_TYPE_DOUBLE

    cdef void append_raw(self, bson_iter_t * doc_iter, bson_type_t value_t) except *:
        if (value_t == BSON_TYPE_DOUBLE or
                    value_t == BSON_TYPE_BOOL or
                    value_t == BSON_TYPE_INT32 or
                    value_t == BSON_TYPE_INT64):
            self.builder.get().Append(bson_iter_as_double(doc_iter))
        else:
            self.builder.get().AppendNull()

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

    cdef void append_raw(self, bson_iter_t * doc_iter, bson_type_t value_t) except *:
        if value_t == BSON_TYPE_DATE_TIME:
            self.builder.get().Append(bson_iter_date_time(doc_iter))
        else:
            self.builder.get().AppendNull()

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

    cdef void append_raw(self, bson_iter_t * doc_iter, bson_type_t value_t) except *:
        if value_t == BSON_TYPE_DATE_TIME:
            self.builder.get().Append(bson_iter_date_time(doc_iter))
        else:
            self.builder.get().AppendNull()

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

    cdef void append_raw(self, bson_iter_t * doc_iter, bson_type_t value_t) except *:
        cdef int64_t value
        cdef int32_t seconds_val

        if value_t == BSON_TYPE_DATE_TIME:
            value = bson_iter_date_time(doc_iter)
            # Convert from milliseconds to days (1000*60*60*24)
            seconds_val = value // 86400000
            self.builder.get().Append(seconds_val)
        else:
            self.builder.get().AppendNull()

    @property
    def unit(self):
        return self.dtype

    cdef shared_ptr[CArrayBuilder] get_builder(self):
        return <shared_ptr[CArrayBuilder]>self.builder

cdef class NullBuilder(_ArrayBuilderBase):
    cdef shared_ptr[CArrayBuilder] builder

    def __cinit__(self, uint8_t count, MemoryPool memory_pool=None):
        cdef CMemoryPool* pool = maybe_unbox_memory_pool(memory_pool)
        cdef uint8_t i
        self.builder.reset(new CNullBuilder(pool))
        self.type_marker = ARROW_TYPE_NULL
        for i in range(count):
            self.append_null()

    cdef void append_raw(self, bson_iter_t * doc_iter, bson_type_t value_t) except *:
        self.builder.get().AppendNull()

    cdef shared_ptr[CArrayBuilder] get_builder(self):
        return <shared_ptr[CArrayBuilder]>self.builder


cdef class BoolBuilder(_ArrayBuilderBase):
    cdef shared_ptr[CBooleanBuilder] builder

    def __cinit__(self, MemoryPool memory_pool=None):
        cdef CMemoryPool* pool = maybe_unbox_memory_pool(memory_pool)
        self.builder.reset(new CBooleanBuilder(pool))
        self.type_marker = BSON_TYPE_BOOL

    cdef void append_raw(self, bson_iter_t * doc_iter, bson_type_t value_t) except *:
        if value_t == BSON_TYPE_BOOL:
            self.builder.get().Append(bson_iter_bool(doc_iter))
        else:
            self.builder.get().AppendNull()

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

    cdef void append_raw(self, bson_iter_t * doc_iter, bson_type_t value_t) except *:
        cdef uint8_t dec128_buf[16]
        cdef bson_decimal128_t dec128

        if self.supported == 0:
            # We do not support big-endian systems.
            self.builder.get().AppendNull()
            return

        if value_t == BSON_TYPE_DECIMAL128:
            bson_iter_decimal128(doc_iter, &dec128)
            memcpy(dec128_buf, &dec128.low, 8);
            memcpy(dec128_buf + 8, &dec128.high, 8)
            self.builder.get().Append(dec128_buf)
        else:
            self.builder.get().AppendNull()

    cdef shared_ptr[CArrayBuilder] get_builder(self):
        return <shared_ptr[CArrayBuilder]>self.builder

    def finish(self):
        return super().finish().cast(Decimal128Type_())


cdef class BinaryBuilder(_ArrayBuilderBase):
    cdef:
        uint8_t _subtype
        shared_ptr[CBinaryBuilder] builder

    def __cinit__(self, uint8_t subtype):
        self._subtype = subtype
        self.builder.reset(new CBinaryBuilder())
        self.type_marker = BSON_TYPE_BINARY

    @property
    def subtype(self):
        return self._subtype

    cdef void append_raw(self, bson_iter_t * doc_iter, bson_type_t value_t) except *:
        cdef const char * val_buf
        cdef uint32_t val_buf_len
        cdef bson_subtype_t subtype

        if value_t == BSON_TYPE_BINARY:
            bson_iter_binary(doc_iter, &subtype, &val_buf_len, <const uint8_t **>&val_buf)
            if subtype != self._subtype:
                self.builder.get().AppendNull()
            else:
                self.builder.get().Append(val_buf, val_buf_len)
        else:
            self.builder.get().AppendNull()

    cdef shared_ptr[CArrayBuilder] get_builder(self):
        return <shared_ptr[CArrayBuilder]>self.builder

    def finish(self):
        return super().finish().cast(BinaryType(self._subtype))


cdef class DocumentBuilder(_ArrayBuilderBase):
    """The document builder stores a map of field names that can be retrieved as a set."""
    cdef:
        dict field_map
        int32_t count

    def __cinit__(self):
        self.type_marker = BSON_TYPE_DOCUMENT
        self.field_map = dict()

    cdef void append_raw(self, bson_iter_t * doc_iter, bson_type_t value_t) except *:
        self.count += 1

    cpdef uint32_t length(self):
        return self.count

    cpdef void append_null(self):
        self.count += 1

    cpdef void add_field(self, cstring field_name):
        self.field_map[field_name] = 1

    def finish(self):
        return set((f.decode('utf-8') for f in self.field_map))


cdef class ListBuilder(_ArrayBuilderBase):
    """The list builder stores an int32 list of offsets and a counter with the current value."""
    cdef:
        int32_t count
        shared_ptr[CInt32Builder] builder

    def __cinit__(self, MemoryPool memory_pool=None):
        cdef CMemoryPool* pool = maybe_unbox_memory_pool(memory_pool)
        self.builder.reset(new CInt32Builder(pool))
        self.count = 0
        self.type_marker = BSON_TYPE_ARRAY

    cdef void append_raw(self, bson_iter_t * doc_iter, bson_type_t value_t) except *:
        self.builder.get().Append(self.count)

    cpdef void append_count(self):
        self.count += 1

    cpdef void append_null(self):
        self.builder.get().Append(self.count)

    cdef shared_ptr[CArrayBuilder] get_builder(self):
        return <shared_ptr[CArrayBuilder]>self.builder

    def finish(self):
        self.builder.get().Append(self.count)
        return super().finish()
