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
import copy
import datetime
import enum
import sys

# Python imports
import bson
import numpy as np
from pyarrow import timestamp, struct, field
from pyarrow.lib import (
    tobytes, StructType, int32, int64, float64, string, bool_, list_
)

from pymongoarrow.errors import InvalidBSON, PyMongoArrowError
from pymongoarrow.context import PyMongoArrowContext
from pymongoarrow.types import _BsonArrowTypes, _atypes, ObjectIdType, Decimal128Type as Decimal128Type_, BinaryType, CodeType

# Cython imports
from cpython cimport object, PyBytes_Size
from cython.operator cimport dereference, preincrement
from libcpp.map cimport map
from libcpp cimport bool as cbool
from libc.math cimport isnan
from libcpp.string cimport string as cstring
from libc.string cimport strlen, memcpy
from pyarrow.lib cimport *
from pymongoarrow.libarrow cimport *
from pymongoarrow.libbson cimport *

# Placeholder numbers for the date types.
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
        map[cstring, void*] builder_map
        uint32_t count
        bint has_schema
        object tzinfo

    def __cinit__(self, object builder_map, bint has_schema, object tzinfo):
        self.has_schema = has_schema
        self.tzinfo = tzinfo
        self.count = 0

        # Build up a map of the builders.
        for key, value in builder_map.items():
            self.builder_map[key] = <void *>value

    cdef _ArrayBuilderBase get_builder(self, cstring key, bson_type_t value_t, bson_iter_t * doc_iter):
        cdef _ArrayBuilderBase builder = None
        cdef bson_subtype_t subtype
        cdef const uint8_t *val_buf = NULL
        cdef uint32_t val_buf_len = 0

        # Handle the builders.
        if value_t == BSON_TYPE_DATE_TIME:
            if self.tzinfo is not None:
                arrow_type = timestamp('ms', tz=self.tzinfo)
                builder = DatetimeBuilder(dtype=arrow_type)
            else:
                builder = DatetimeBuilder()
        elif value_t == BSON_TYPE_DOCUMENT:
            builder = DocumentBuilder(self, key)
        elif value_t == BSON_TYPE_ARRAY:
            builder = ListBuilder(self, key)
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

        # If we still haven't found a builder, leave it as None until we find it.
        self.builder_map[key] = <void *>builder
        return builder

    cdef uint8_t parse_document(self, const uint8_t * docstream, size_t length, cstring base_key, uint8_t parent_type) except *:
        cdef bson_reader_t* stream_reader = bson_reader_new_from_data(docstream, length)
        cdef bson_type_t value_t
        cdef const bson_t * doc = NULL
        cdef bson_iter_t doc_iter
        cdef cstring key
        cdef cstring full_key
        cdef uint32_t i
        cdef uint32_t builder_len
        cdef uint32_t diff
        cdef _ArrayBuilderBase builder = None
        cdef map[cstring, void*].iterator it

        try:
            doc = bson_reader_read_safe(stream_reader)
            if doc == NULL:
                return 1

            if not bson_iter_init(&doc_iter, doc):
                raise InvalidBSON("Could not read BSON document")

            while bson_iter_next(&doc_iter):
                key = bson_iter_key(&doc_iter)

                # Get the appropriate full key.
                if parent_type == BSON_TYPE_ARRAY:
                    full_key = base_key
                    full_key.append(b"[]")

                elif parent_type == BSON_TYPE_DOCUMENT:
                    full_key = base_key
                    full_key.append(b".")
                    full_key.append(key)
                    (<DocumentBuilder>self.builder_map[base_key]).add_field_raw(key)

                else:
                    full_key = key

                # Get the builder.
                value_t = bson_iter_type(&doc_iter)

                it = self.builder_map.find(full_key)
                if it != self.builder_map.end():
                    builder = <_ArrayBuilderBase>self.builder_map[key]

                if builder is None and self.has_schema:
                    continue

                # Mark a null key as missing until we find it.
                if builder is None and value_t == BSON_TYPE_NULL:
                    self.builder_map[key] = <void *>NULL
                    continue

                if builder is None:
                    builder = self.get_builder(full_key, value_t, &doc_iter)

                if builder is None:
                    continue

                # Append nulls to catch up.
                builder_len = builder.length()
                diff = self.count - builder_len
                if diff > 0:
                    for i in range(diff):
                        builder.append_null()
                        if parent_type == BSON_TYPE_ARRAY:
                            (<ListBuilder>self.builder_map[base_key]).append_count_raw()
                    builder_len = self.count

                # Append the next value.
                builder.append_raw(&doc_iter, value_t)
                if parent_type == BSON_TYPE_ARRAY:
                    (<ListBuilder>self.builder_map[base_key])

                # Update our count.
                if builder_len + 1 > self.count:
                    self.count = builder_len + 1

            return 0
        finally:
            bson_reader_destroy(stream_reader)

    cpdef void process_bson_stream(self, const uint8_t* bson_stream, size_t length):
        """Process a bson byte stream."""
        cdef uint8_t ret
        while True:
            ret = self.parse_document(bson_stream, length, b"", 0)
            if ret == 1:
                break

    cpdef finish(self):
        """Finish building the arrays."""
        cdef map[cstring, void*].iterator it
        cdef _ArrayBuilderBase builder
        builder_map = dict()

        it = self.builder_map.begin()
        while it != self.builder_map.end():
            key = dereference(it).first
            if dereference(it).second == NULL:
                builder_map[key] = NullBuilder(self.count)
            else:
                builder = <_ArrayBuilderBase>self.builder_map[key]
                builder_map[key] = builder
            preincrement(it)

        return builder_map

cdef class _ArrayBuilderBase:
    cdef:
        uint8_t type_marker

    cpdef void append(self, object value):
        """Interface to append a python value to the builder.
        """
        cdef bson_reader_t* stream_reader = NULL
        cdef const bson_t * doc = NULL
        cdef bson_iter_t doc_iter

        data = bson.encode(dict(data=value))
        stream_reader = bson_reader_new_from_data(data[:], len(data))
        doc = bson_reader_read_safe(stream_reader)
        if doc == NULL:
            raise ValueError("Could not append", value)
        if not bson_iter_init(&doc_iter, doc):
            raise InvalidBSON("Could not read BSON document")
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

    cdef uint8_t length(self):
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
        return super().finish().cast(CodeType)


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
        map[cstring, int32_t] field_map
        BuilderManager manager
        cstring key
        int32_t count

    def __cinit__(self, BuilderManager manager, cstring key):
        self.type_marker = BSON_TYPE_DOCUMENT
        self.manager = manager
        self.key = key

    cdef void append_raw(self, bson_iter_t * doc_iter, bson_type_t value_t) except *:
        cdef const uint8_t *val_buf = NULL
        cdef uint32_t val_buf_len = 0
        self.count += 1
        if value_t == BSON_TYPE_DOCUMENT:
            bson_iter_document(doc_iter, &val_buf_len, &val_buf)
            if val_buf_len <= 0:
                raise ValueError("Subdocument is invalid")
            self.manager.parse_document(val_buf, val_buf_len, self.key, BSON_TYPE_DOCUMENT)

    cpdef void append_null(self):
        self.count += 1

    cdef void add_field_raw(self, cstring field):
        self.field_map[field] = 1

    cpdef void add_field(self, field):
        self.field_map[field] = 1

    def finish(self):
        it = self.field_map.begin()
        names = set()
        while it != self.field_map.end():
            names.add(dereference(it).first)
            preincrement(it)
        return names


cdef class ListBuilder(_ArrayBuilderBase):
    """The list builder stores an int32 list of offsets and a counter with the current value."""
    cdef:
        BuilderManager manager
        int32_t count
        cstring key
        shared_ptr[CInt32Builder] builder

    def __cinit__(self, BuilderManager manager, cstring key, MemoryPool memory_pool=None):
        cdef CMemoryPool* pool = maybe_unbox_memory_pool(memory_pool)
        self.builder.reset(new CInt32Builder(pool))
        self.count = 0
        self.type_marker = BSON_TYPE_ARRAY
        self.manager = manager
        self.key = key

    cdef void append_raw(self, bson_iter_t * doc_iter, bson_type_t value_t) except *:
        cdef const uint8_t *val_buf = NULL
        cdef uint32_t val_buf_len = 0
        self.builder.get().Append(self.count)
        if value_t == BSON_TYPE_ARRAY:
            bson_iter_array(doc_iter, &val_buf_len, &val_buf)
            if val_buf_len <= 0:
                raise ValueError("Subarray is invalid")
            self.manager.parse_document(val_buf, val_buf_len, self.key, BSON_TYPE_ARRAY)

    cdef void append_count_raw(self):
        self.count +=1

    cpdef void append_count(self):
        self.count += 1

    cpdef void append_null(self):
        self.builder.get().Append(self.count)

    cdef shared_ptr[CArrayBuilder] get_builder(self):
        return <shared_ptr[CArrayBuilder]>self.builder
