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
from math import isnan

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
from cpython cimport PyBytes_Size, object
from cython.operator cimport dereference, preincrement
from libcpp cimport bool as cbool
from libcpp.map cimport map
from libcpp.string cimport string as cstring
from libc.string cimport strlen, memcpy
from libcpp.vector cimport vector
from pyarrow.lib cimport *
from pymongoarrow.libarrow cimport *
from pymongoarrow.libbson cimport *


# libbson version
libbson_version = bson_get_version().decode('utf-8')

# BSON tools

cdef const bson_t* bson_reader_read_safe(bson_reader_t* stream_reader) except? NULL:
    cdef cbool reached_eof = False
    cdef const bson_t* doc = bson_reader_read(stream_reader, &reached_eof)
    if doc == NULL and reached_eof is False:
        raise InvalidBSON("Could not read BSON document stream")
    return doc


# Placeholder numbers for the date types.
cdef uint8_t ARROW_TYPE_DATE32 = 100
cdef uint8_t ARROW_TYPE_DATE64 = 101
cdef uint8_t ARROW_TYPE_NULL = 102

_builder_type_map = {
    BSON_TYPE_INT32: Int32Builder,
    BSON_TYPE_INT64: Int64Builder,
    BSON_TYPE_DOUBLE: DoubleBuilder,
    BSON_TYPE_DATE_TIME: DatetimeBuilder,
    BSON_TYPE_OID: ObjectIdBuilder,
    BSON_TYPE_UTF8: StringBuilder,
    BSON_TYPE_BOOL: BoolBuilder,
    BSON_TYPE_DOCUMENT: DocumentBuilder,
    BSON_TYPE_DECIMAL128: Decimal128Builder,
    BSON_TYPE_ARRAY: ListBuilder,
    BSON_TYPE_BINARY: BinaryBuilder,
    BSON_TYPE_CODE: CodeBuilder,
    ARROW_TYPE_DATE32: Date32Builder,
    ARROW_TYPE_DATE64: Date64Builder,
    ARROW_TYPE_NULL: NullBuilder
}

_field_type_map = {
    BSON_TYPE_INT32: int32(),
    BSON_TYPE_INT64: int64(),
    BSON_TYPE_DOUBLE: float64(),
    BSON_TYPE_OID: ObjectIdType(),
    BSON_TYPE_UTF8: string(),
    BSON_TYPE_BOOL: bool_(),
    BSON_TYPE_DECIMAL128: Decimal128Type_(),
    BSON_TYPE_CODE: CodeType(),
}


def process_bson_stream(bson_stream, context, arr_value_builder=None):
    """Process a bson byte stream using a PyMongoArrowContext"""
    cdef const uint8_t* docstream = <const uint8_t *>bson_stream
    cdef size_t length = <size_t>PyBytes_Size(bson_stream)
    cdef map[cstring, void *] builder_map
    cdef uint8_t ret

    # Build up a map of the builders.
    for key, value in context.builder_map.items():
        builder_map[key] = <void *>value

    try:
        while True:
            ret = parse_document(docstream, length, context, builder_map, "", 0)
            if ret == 1:
                break
    finally:
        bson_reader_destroy(stream_reader)

    # Any missing fields that are left must be null fields.
    # TODO: handle missing builders as NULL values in the builder map.
    it = missing_builders.begin()
    while it != missing_builders.end():
        builder = NullBuilder()
        key = dereference(it).first
        context.builder_map[key] = builder
        null_builder = builder
        for _ in range(count):
            null_builder.append_null()
        preincrement(it)


cdef void get_builder(cdef cstring key, object context, map[cstring, void *] builder_map) except *:
    cdef map[cstring, void*].iterator it
    cdef _ArrayBuilderBase builder = None

    it = builder_map.find(key)
    if it != builder_map.end():
        builder = <_ArrayBuilderBase>builder_map[key]

    if builder is None and context.schema is not None:
        return

    if builder is None:
        # Get the appropriate builder for the current field.
        value_t = bson_iter_type(&doc_iter)
        builder_type = _builder_type_map.get(value_t)

        # Mark the key as missing until we find it.
        if builder_type is None:
            builder_map[key] =  <void *>None
            return

    # Handle the parameterized builders.
    if builder_type == DatetimeBuilder and context.tzinfo is not None:
        arrow_type = timestamp('ms', tz=context.tzinfo)
        builder = DatetimeBuilder(dtype=arrow_type)
    elif builder_type == BinaryBuilder:
        bson_iter_binary (&doc_iter, &subtype,
            &val_buf_len, &val_buf)
        builder = BinaryBuilder(subtype)
    elif builder_type == Date32Builder:
        builder = Date32Builder()
    elif builder_type == Date64Builder:
        builder = Date64Builder()
    else:
        builder = builder_type()

    builder_map[key] = <void *>builder
    context.builder_map[key] = builder

    # TODO: derive count from the builder map
    for _ in range(count):
        builder.append_null()

    return builder


"""
Questions:
How to handle null document and list values? - test with arrow-165.py
"""

cdef void parse_document(const uint8_t * docstream, size_t length, object context, map[cstring, void *] builder_map, cstring base_key, uint8_t parent_type) except *:
    cdef bson_reader_t* stream_reader = bson_reader_new_from_data(docstream, length)
    cdef const bson_t * doc = NULL
    cdef bson_iter_t doc_iter
    cdef cstring key
    cdef uint32_t str_len
    cdef uint8_t dec128_buf[16]
    cdef const uint8_t *val_buf = NULL
    cdef uint32_t val_buf_len = 0
    cdef bson_decimal128_t dec128
    cdef bson_type_t value_t
    cdef const char * bson_str = NULL
    cdef StructType struct_dtype
    cdef const bson_t * doc = NULL
    cdef bson_iter_t doc_iter
    cdef bson_iter_t child_iter
    cdef cstring key
    cdef uint8_t ftype
    cdef Py_ssize_t count = 0
    cdef uint8_t byte_order_status = 0
    cdef map[cstring, void *] builder_map
    cdef map[cstring, void *] missing_builders
    cdef map[cstring, void*].iterator it
    cdef bson_subtype_t subtype
    cdef int32_t val32
    cdef int64_t val64

    cdef _ArrayBuilderBase builder = None
    cdef _ArrayBuilderBase parent_builder = None
    cdef Int32Builder int32_builder
    cdef DoubleBuilder double_builder
    cdef ObjectIdBuilder objectid_builder
    cdef StringBuilder string_builder
    cdef CodeBuilder code_builder
    cdef Int64Builder int64_builder
    cdef BoolBuilder bool_builder
    cdef BinaryBuilder binary_builder
    cdef DatetimeBuilder datetime_builder
    cdef Decimal128Builder dec128_builder
    cdef Date32Builder date32_builder
    cdef Date64Builder date64_builder
    cdef NullBuilder null_builder

    doc = bson_reader_read_safe(stream_reader)
    if doc == NULL:
        return 1

    if not bson_iter_init(&doc_iter, doc):
        raise InvalidBSON("Could not read BSON document")

    # Container logic - TODO make this work with c
    if base_key and base_key in builder_map:
        parent_builder = builder_map[base_key]
    if parent_type == BSON_TYPE_ARRAY:
        parent_builder.append_offset()

    while bson_iter_next(&doc_iter):
        # TODO: make this work with c types
        key = bson_iter_key(&doc_iter)

        # Container item logic - TODO: make this work with c types.
        if parent_type == BSON_TYPE_ARRAY:
            full_key = base_key + '[]'
            parent_builder.append()

        elif parent_type == BSON_TYPE_DOCUMENT:
            full_key = f'{base_key}.{key}'
            parent_builder.add_child(key)

        else:
            full_key = key

        builder = get_builder(key, context, builder_map)
        if builder is None:
            continue

        ftype = builder.type_marker
        value_t = bson_iter_type(&doc_iter)
        if ftype == BSON_TYPE_INT32:
            int32_builder = builder
            if (value_t == BSON_TYPE_INT32 or value_t == BSON_TYPE_BOOL):
                int32_builder.append_raw(bson_iter_as_int64(&doc_iter))
            elif value_t == BSON_TYPE_INT64:
                val64 = bson_iter_as_int64(&doc_iter)
                val32 = <int32_t> val64
                if val64 == val32:
                    int32_builder.append_raw(val32)
                else:
                    # Use append (not append_raw) to surface overflow errors.
                    int32_builder.append(val64)
            elif value_t == BSON_TYPE_DOUBLE:
                # Treat nan as null.
                val = bson_iter_as_double(&doc_iter)
                if isnan(val):
                    int32_builder.append_null()
                else:
                    # Use append (not append_raw) to surface overflow errors.
                    int32_builder.append(bson_iter_as_int64(&doc_iter))
            else:
                int32_builder.append_null()
        elif ftype == BSON_TYPE_INT64:
            int64_builder = builder
            if (value_t == BSON_TYPE_INT64 or
                    value_t == BSON_TYPE_BOOL or
                    value_t == BSON_TYPE_INT32):
                int64_builder.append_raw(bson_iter_as_int64(&doc_iter))
            elif value_t == BSON_TYPE_DOUBLE:
                # Treat nan as null.
                val = bson_iter_as_double(&doc_iter)
                if isnan(val):
                    int64_builder.append_null()
                else:
                    int64_builder.append_raw(bson_iter_as_int64(&doc_iter))
            else:
                int64_builder.append_null()
        elif ftype == BSON_TYPE_OID:
            objectid_builder = builder
            if value_t == BSON_TYPE_OID:
                objectid_builder.append_raw(bson_iter_oid(&doc_iter))
            else:
                objectid_builder.append_null()
        elif ftype == BSON_TYPE_UTF8:
            string_builder = builder
            if value_t == BSON_TYPE_UTF8:
                bson_str = bson_iter_utf8(&doc_iter, &str_len)
                string_builder.append_raw(bson_str, str_len)
            else:
                string_builder.append_null()
        elif ftype == BSON_TYPE_CODE:
            code_builder = builder
            if value_t == BSON_TYPE_CODE:
                bson_str = bson_iter_code(&doc_iter, &str_len)
                code_builder.append_raw(bson_str, str_len)
            else:
                code_builder.append_null()
        elif ftype == BSON_TYPE_DECIMAL128:
            dec128_builder = builder
            if value_t == BSON_TYPE_DECIMAL128:
                bson_iter_decimal128(&doc_iter, &dec128)
                if byte_order_status == 0:
                    if sys.byteorder == 'little':
                        byte_order_status = 1
                    else:
                        byte_order_status = 2
                if byte_order_status == 1:
                    memcpy(dec128_buf, &dec128.low, 8);
                    memcpy(dec128_buf + 8, &dec128.high, 8)
                    dec128_builder.append_raw(dec128_buf)
                else:
                    # We do not support big-endian systems.
                    dec128_builder.append_null()
            else:
                dec128_builder.append_null()
        elif ftype == BSON_TYPE_DOUBLE:
            double_builder = builder
            if (value_t == BSON_TYPE_DOUBLE or
                    value_t == BSON_TYPE_BOOL or
                    value_t == BSON_TYPE_INT32 or
                    value_t == BSON_TYPE_INT64):
                double_builder.append_raw(bson_iter_as_double(&doc_iter))
            else:
                double_builder.append_null()
        elif ftype == ARROW_TYPE_DATE32:
            date32_builder = builder
            if value_t == BSON_TYPE_DATE_TIME:
                date32_builder.append_raw(bson_iter_date_time(&doc_iter))
            else:
                date32_builder.append_null()
        elif ftype == ARROW_TYPE_DATE64:
            date64_builder = builder
            if value_t == BSON_TYPE_DATE_TIME:
                date64_builder.append_raw(bson_iter_date_time(&doc_iter))
            else:
                date64_builder.append_null()
        elif ftype == BSON_TYPE_DATE_TIME:
            datetime_builder = builder
            if value_t == BSON_TYPE_DATE_TIME:
                datetime_builder.append_raw(bson_iter_date_time(&doc_iter))
            else:
                datetime_builder.append_null()
        elif ftype == BSON_TYPE_BOOL:
            bool_builder = builder
            if value_t == BSON_TYPE_BOOL:
                bool_builder.append_raw(bson_iter_bool(&doc_iter))
            else:
                bool_builder.append_null()
        elif ftype == BSON_TYPE_DOCUMENT:
            doc_builder = builder
            if value_t == BSON_TYPE_DOCUMENT:
                bson_iter_document(&doc_iter, &val_buf_len, &val_buf)
                if val_buf_len <= 0:
                    raise ValueError("Subdocument is invalid")
                # TODO: make this work with c types
                subkey = key + "."
                parse_document(val_buf, val_buf_len, context, builder_map, subkey, BSON_TYPE_DOCUMENT)
            else:
                doc_builder.append_null()
        elif ftype == BSON_TYPE_ARRAY:
            list_builder = builder
            if value_t == BSON_TYPE_ARRAY:
                bson_iter_array(&doc_iter, &val_buf_len, &val_buf)
                if val_buf_len <= 0:
                    raise ValueError("Subarray is invalid")
                # TODO: make this work with c types
                subkey = key + "[]"
                parse_document(val_buf, val_buf_len, context, builder_map, subkey, BSON_TYPE_ARRAY)
            else:
                list_builder.append_null()
        elif ftype == BSON_TYPE_BINARY:
            binary_builder = builder
            if value_t == BSON_TYPE_BINARY:
                bson_iter_binary (&doc_iter, &subtype,
                    &val_buf_len, &val_buf)
                if subtype != binary_builder._subtype:
                    binary_builder.append_null()
                else:
                    binary_builder.append_raw(<char*>val_buf, val_buf_len)
        elif ftype == ARROW_TYPE_NULL:
            null_builder = builder
            null_builder.append_null()
        else:
            raise PyMongoArrowError('unknown ftype {}'.format(ftype))
    return 0


# Builders

cdef class _ArrayBuilderBase:
    cdef uint8_t type_marker

    cpdef append_values(self, values):
        for value in values:
            if value is None or value is np.nan:
                self.append_null()
            else:
                self.append(value)


cdef class StringBuilder(_ArrayBuilderBase):
    cdef:
        shared_ptr[CStringBuilder] builder

    def __cinit__(self, MemoryPool memory_pool=None):
        cdef CMemoryPool* pool = maybe_unbox_memory_pool(memory_pool)
        self.builder.reset(new CStringBuilder(pool))
        self.type_marker = BSON_TYPE_UTF8

    cdef append_raw(self, const char * value, uint32_t str_len):
        self.builder.get().Append(value, str_len)

    cpdef append_null(self):
        self.builder.get().AppendNull()

    cpdef append(self, value):
        value = tobytes(value)
        self.append_raw(value, len(value))

    def __len__(self):
        return self.builder.get().length()

    cpdef finish(self):
        cdef shared_ptr[CArray] out
        with nogil:
            self.builder.get().Finish(&out)
        return pyarrow_wrap_array(out)

    cdef shared_ptr[CStringBuilder] unwrap(self):
        return self.builder


cdef class CodeBuilder(StringBuilder):
    def __cinit__(self, MemoryPool memory_pool=None):
        cdef CMemoryPool* pool = maybe_unbox_memory_pool(memory_pool)
        self.builder.reset(new CStringBuilder(pool))
        self.type_marker = BSON_TYPE_CODE

    cpdef finish(self):
        cdef shared_ptr[CArray] out
        with nogil:
            self.builder.get().Finish(&out)
        return pyarrow_wrap_array(out).cast(CodeType())


cdef class ObjectIdBuilder(_ArrayBuilderBase):
    cdef:
        shared_ptr[CFixedSizeBinaryBuilder] builder

    def __cinit__(self, MemoryPool memory_pool=None):
        cdef shared_ptr[CDataType] dtype = fixed_size_binary(12)
        cdef CMemoryPool* pool = maybe_unbox_memory_pool(memory_pool)
        self.builder.reset(new CFixedSizeBinaryBuilder(dtype, pool))
        self.type_marker = BSON_TYPE_OID

    cdef append_raw(self, const bson_oid_t * value):
        self.builder.get().Append(value.bytes)

    cpdef append(self, value):
        self.builder.get().Append(value)

    cpdef append_null(self):
        self.builder.get().AppendNull()

    def __len__(self):
        return self.builder.get().length()

    cpdef finish(self):
        cdef shared_ptr[CArray] out
        with nogil:
            self.builder.get().Finish(&out)
        return pyarrow_wrap_array(out).cast(ObjectIdType())

    cdef shared_ptr[CFixedSizeBinaryBuilder] unwrap(self):
        return self.builder


cdef class Int32Builder(_ArrayBuilderBase):
    cdef:
        shared_ptr[CInt32Builder] builder

    def __cinit__(self, MemoryPool memory_pool=None):
        cdef CMemoryPool* pool = maybe_unbox_memory_pool(memory_pool)
        self.builder.reset(new CInt32Builder(pool))
        self.type_marker = BSON_TYPE_INT32

    cdef append_raw(self, int32_t value):
        self.builder.get().Append(value)

    cpdef append(self, value):
        self.builder.get().Append(value)

    cpdef append_null(self):
        self.builder.get().AppendNull()

    def __len__(self):
        return self.builder.get().length()

    cpdef finish(self):
        cdef shared_ptr[CArray] out
        with nogil:
            self.builder.get().Finish(&out)
        return pyarrow_wrap_array(out)

    cdef shared_ptr[CInt32Builder] unwrap(self):
        return self.builder


cdef class Int64Builder(_ArrayBuilderBase):
    cdef:
        shared_ptr[CInt64Builder] builder

    def __cinit__(self, MemoryPool memory_pool=None):
        cdef CMemoryPool* pool = maybe_unbox_memory_pool(memory_pool)
        self.builder.reset(new CInt64Builder(pool))
        self.type_marker = BSON_TYPE_INT64

    cdef append_raw(self, int64_t value):
        self.builder.get().Append(value)

    cpdef append(self, value):
        self.builder.get().Append(value)

    cpdef append_null(self):
        self.builder.get().AppendNull()

    def __len__(self):
        return self.builder.get().length()

    cpdef finish(self):
        cdef shared_ptr[CArray] out
        with nogil:
            self.builder.get().Finish(&out)
        return pyarrow_wrap_array(out)

    cdef shared_ptr[CInt64Builder] unwrap(self):
        return self.builder


cdef class DoubleBuilder(_ArrayBuilderBase):
    cdef:
        shared_ptr[CDoubleBuilder] builder

    def __cinit__(self, MemoryPool memory_pool=None):
        cdef CMemoryPool* pool = maybe_unbox_memory_pool(memory_pool)
        self.builder.reset(new CDoubleBuilder(pool))
        self.type_marker = BSON_TYPE_DOUBLE

    cdef append_raw(self, double value):
        self.builder.get().Append(value)

    cpdef append(self, value):
        self.builder.get().Append(value)

    cpdef append_null(self):
        self.builder.get().AppendNull()

    def __len__(self):
        return self.builder.get().length()

    cpdef finish(self):
        cdef shared_ptr[CArray] out
        with nogil:
            self.builder.get().Finish(&out)
        return pyarrow_wrap_array(out)

    cdef shared_ptr[CDoubleBuilder] unwrap(self):
        return self.builder


cdef class DatetimeBuilder(_ArrayBuilderBase):
    cdef:
        shared_ptr[CTimestampBuilder] builder
        TimestampType dtype

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

    cdef append_raw(self, int64_t value):
        self.builder.get().Append(value)

    cpdef append(self, value):
        self.builder.get().Append(value)

    cpdef append_null(self):
        self.builder.get().AppendNull()

    def __len__(self):
        return self.builder.get().length()

    @property
    def unit(self):
        return self.dtype

    cpdef finish(self):
        cdef shared_ptr[CArray] out
        with nogil:
            self.builder.get().Finish(&out)
        return pyarrow_wrap_array(out)

    cdef shared_ptr[CTimestampBuilder] unwrap(self):
        return self.builder

cdef class Date64Builder(_ArrayBuilderBase):
    cdef:
        shared_ptr[CDate64Builder] builder
        DataType dtype

    def __cinit__(self, MemoryPool memory_pool=None):
        cdef CMemoryPool* pool = maybe_unbox_memory_pool(memory_pool)
        self.builder.reset(new CDate64Builder(pool))
        self.type_marker = ARROW_TYPE_DATE64

    cdef append_raw(self, int64_t value):
        self.builder.get().Append(value)

    cpdef append(self, value):
        self.builder.get().Append(value)

    cpdef append_null(self):
        self.builder.get().AppendNull()

    def __len__(self):
        return self.builder.get().length()

    @property
    def unit(self):
        return self.dtype

    cpdef finish(self):
        cdef shared_ptr[CArray] out
        with nogil:
            self.builder.get().Finish(&out)
        return pyarrow_wrap_array(out)

    cdef shared_ptr[CDate64Builder] unwrap(self):
        return self.builder

cdef class Date32Builder(_ArrayBuilderBase):
    cdef:
        shared_ptr[CDate32Builder] builder
        DataType dtype

    def __cinit__(self, MemoryPool memory_pool=None):
        cdef CMemoryPool* pool = maybe_unbox_memory_pool(memory_pool)
        self.builder.reset(new CDate32Builder(pool))
        self.type_marker = ARROW_TYPE_DATE32

    cdef append_raw(self, int64_t value):
        # Convert from milliseconds to days (1000*60*60*24)
        cdef int32_t seconds_val = value // 86400000
        self.builder.get().Append(seconds_val)

    cpdef append(self, value):
        self.builder.get().Append(value)

    cpdef append_null(self):
        self.builder.get().AppendNull()

    def __len__(self):
        return self.builder.get().length()

    @property
    def unit(self):
        return self.dtype

    cpdef finish(self):
        cdef shared_ptr[CArray] out
        with nogil:
            self.builder.get().Finish(&out)
        return pyarrow_wrap_array(out)

    cdef shared_ptr[CDate32Builder] unwrap(self):
        return self.builder


cdef class NullBuilder(_ArrayBuilderBase):
    cdef:
        shared_ptr[CNullBuilder] builder

    def __cinit__(self, MemoryPool memory_pool=None):
        cdef CMemoryPool* pool = maybe_unbox_memory_pool(memory_pool)
        self.builder.reset(new CNullBuilder(pool))
        self.type_marker = ARROW_TYPE_NULL

    cdef append_raw(self, void* value):
        self.builder.get().AppendNull()

    cpdef append(self, value):
        self.builder.get().AppendNull()

    cpdef append_null(self):
        self.builder.get().AppendNull()

    def __len__(self):
        return self.builder.get().length()

    cpdef finish(self):
        cdef shared_ptr[CArray] out
        with nogil:
            self.builder.get().Finish(&out)
        return pyarrow_wrap_array(out)

    cdef shared_ptr[CNullBuilder] unwrap(self):
        return self.builder


cdef class BoolBuilder(_ArrayBuilderBase):
    cdef:
        shared_ptr[CBooleanBuilder] builder

    def __cinit__(self, MemoryPool memory_pool=None):
        cdef CMemoryPool* pool = maybe_unbox_memory_pool(memory_pool)
        self.builder.reset(new CBooleanBuilder(pool))
        self.type_marker = BSON_TYPE_BOOL

    cdef append_raw(self, cbool value):
        self.builder.get().Append(value)

    cpdef append(self, cbool value):
        self.builder.get().Append(value)

    cpdef append_null(self):
        self.builder.get().AppendNull()

    def __len__(self):
        return self.builder.get().length()

    cpdef finish(self):
        cdef shared_ptr[CArray] out
        with nogil:
            self.builder.get().Finish(&out)
        return pyarrow_wrap_array(out)

    cdef shared_ptr[CBooleanBuilder] unwrap(self):
        return self.builder


cdef class Decimal128Builder(_ArrayBuilderBase):
    cdef:
        shared_ptr[CFixedSizeBinaryBuilder] builder

    def __cinit__(self, MemoryPool memory_pool=None):
        cdef shared_ptr[CDataType] dtype = fixed_size_binary(16)
        cdef CMemoryPool* pool = maybe_unbox_memory_pool(memory_pool)
        self.builder.reset(new CFixedSizeBinaryBuilder(dtype, pool))
        self.type_marker = BSON_TYPE_DECIMAL128

    cdef append_raw(self, uint8_t * buf):
        self.builder.get().Append(buf)

    cpdef append(self, value):
        self.builder.get().Append(value)

    cpdef append_null(self):
        self.builder.get().AppendNull()

    def __len__(self):
        return self.builder.get().length()

    cpdef finish(self):
        cdef shared_ptr[CArray] out
        with nogil:
            self.builder.get().Finish(&out)
        return pyarrow_wrap_array(out).cast(Decimal128Type_())

    cdef shared_ptr[CFixedSizeBinaryBuilder] unwrap(self):
        return self.builder


cdef class DocumentBuilder(_ArrayBuilderBase):
    """The document builder stores a map of field names that can be retrieved as a set."""
    cdef:
        map[cstring, int32_t] field_map

    def __cinit__(self):
        self.type_marker = BSON_TYPE_DOCUMENT

    cdef add_field_raw(self, char * field):
        self.field_map[field] = 1

    cpdef add_field(self, field):
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
        shared_ptr[CInt32Builder] builder
        int32_t count

    def __cinit__(self, MemoryPool memory_pool=None):
        cdef CMemoryPool* pool = maybe_unbox_memory_pool(memory_pool)
        self.builder.reset(new CInt32Builder(pool))
        self.count = 0
        self.type_marker = BSON_TYPE_ARRAY

    cdef append_offset_raw(self):
        self.builder.get().Append(self.count)

    cpdef append_offset(self):
        self.builder.get().Append(self.count)

    cdef append_raw(self):
        self.count += 1

    cpdef append(self):
        self.count += 1

    cpdef append_null(self):
        self.builder.get().AppendNull()

    def __len__(self):
        return self.builder.get().length()

    cpdef finish(self):
        self.append_offset()
        cdef shared_ptr[CArray] out
        with nogil:
            self.builder.get().Finish(&out)
        return pyarrow_wrap_array(out)

    cdef shared_ptr[CInt32Builder] unwrap(self):
        return self.builder


cdef class BinaryBuilder(_ArrayBuilderBase):
    cdef:
        shared_ptr[CBinaryBuilder] builder
        uint8_t _subtype

    def __cinit__(self, uint8_t subtype):
        self._subtype = subtype
        self.builder.reset(new CBinaryBuilder())
        self.type_marker = BSON_TYPE_BINARY

    @property
    def subtype(self):
        return self._subtype

    cdef append_raw(self, const char * value, uint32_t str_len):
        self.builder.get().Append(value, str_len)

    cpdef append_null(self):
        self.builder.get().AppendNull()

    cpdef append(self, value):
        self.append_raw(<const char *>value, len(value))

    def __len__(self):
        return self.builder.get().length()

    cpdef finish(self):
        cdef shared_ptr[CArray] out
        with nogil:
            self.builder.get().Finish(&out)
        return pyarrow_wrap_array(out).cast(BinaryType(self._subtype))

    cdef shared_ptr[CBinaryBuilder] unwrap(self):
        return self.builder
