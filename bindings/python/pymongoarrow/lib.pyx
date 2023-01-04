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

# Python imports
import bson
import numpy as np
from pyarrow import timestamp, struct, field
from pyarrow.lib import (
    tobytes, StructType, int32, int64, float64, string, bool_, list_
)

from pymongoarrow.errors import InvalidBSON, PyMongoArrowError
from pymongoarrow.context import PyMongoArrowContext
from pymongoarrow.types import _BsonArrowTypes, _atypes, ObjectIdType, Decimal128StringType

# Cython imports
from cpython cimport PyBytes_Size, object
from cython.operator cimport dereference
from libcpp cimport bool as cbool
from libcpp.map cimport map
from libcpp.vector cimport vector
from libc.stdlib cimport malloc, free
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

_builder_type_map = {
    BSON_TYPE_INT32: Int32Builder,
    BSON_TYPE_INT64: Int64Builder,
    BSON_TYPE_DOUBLE: DoubleBuilder,
    BSON_TYPE_DATE_TIME: DatetimeBuilder,
    BSON_TYPE_OID: ObjectIdBuilder,
    BSON_TYPE_UTF8: StringBuilder,
    BSON_TYPE_BOOL: BoolBuilder,
    BSON_TYPE_DOCUMENT: DocumentBuilder,
    BSON_TYPE_DECIMAL128: StringBuilder,
    BSON_TYPE_ARRAY: ListBuilder,
}

_field_type_map = {
    BSON_TYPE_INT32: int32(),
    BSON_TYPE_INT64: int64(),
    BSON_TYPE_DOUBLE: float64(),
    BSON_TYPE_OID: ObjectIdType(),
    BSON_TYPE_UTF8: string(),
    BSON_TYPE_BOOL: bool_(),
    BSON_TYPE_DECIMAL128: Decimal128StringType(),
}

cdef extract_field_dtype(bson_iter_t * doc_iter, bson_iter_t * child_iter, bson_type_t value_t, context):
    """Get the appropropriate data type for a specific field"""
    if value_t in _field_type_map:
        field_type = _field_type_map[value_t]
    elif value_t == BSON_TYPE_ARRAY:
        bson_iter_recurse(doc_iter, child_iter)
        list_dtype = extract_array_dtype(child_iter, context)
        field_type = list_(list_dtype)
    elif value_t == BSON_TYPE_DOCUMENT:
        bson_iter_recurse(doc_iter, child_iter)
        field_type = extract_document_dtype(child_iter, context)
    elif value_t == BSON_TYPE_DATE_TIME:
        field_type = timestamp('ms', tz=context.tzinfo)
    else:
        raise PyMongoArrowError('unknown value type {}'.format(value_t))
    return field_type


cdef extract_document_dtype(bson_iter_t * doc_iter, context):
    """Get the appropropriate data type for a sub document"""
    cdef const char* key
    cdef bson_type_t value_t
    cdef bson_iter_t child_iter
    fields = []
    while bson_iter_next(doc_iter):
        key = bson_iter_key(doc_iter)
        value_t = bson_iter_type(doc_iter)
        field_type = extract_field_dtype(doc_iter, &child_iter, value_t, context)
        fields.append(field(key.decode('utf-8'), field_type))
    return struct(fields)

cdef extract_array_dtype(bson_iter_t * doc_iter, context):
    """Get the appropropriate data type for a sub array"""
    cdef const char* key
    cdef bson_type_t value_t
    cdef bson_iter_t child_iter
    fields = []
    first_item = bson_iter_next(doc_iter)
    value_t = bson_iter_type(doc_iter)
    return extract_field_dtype(doc_iter, &child_iter, value_t, context)

def process_bson_stream(bson_stream, context, arr_value_builder=None):
    """Process a bson byte stream using a PyMongoArrowContext"""
    cdef const uint8_t* docstream = <const uint8_t *>bson_stream
    cdef size_t length = <size_t>PyBytes_Size(bson_stream)
    cdef bson_reader_t* stream_reader = bson_reader_new_from_data(docstream, length)
    cdef char *decimal128_str = <char *> malloc(
        BSON_DECIMAL128_STRING * sizeof(char))
    cdef uint32_t str_len
    cdef const uint8_t *doc_buf = NULL
    cdef uint32_t doc_buf_len = 0;
    cdef const uint8_t *arr_buf = NULL
    cdef uint32_t arr_buf_len = 0;
    cdef bson_decimal128_t dec128
    cdef bson_type_t value_t
    cdef const char * bson_str
    cdef StructType struct_dtype
    cdef const bson_t * doc = NULL
    cdef bson_iter_t doc_iter
    cdef bson_iter_t child_iter
    cdef const char* key
    cdef Py_ssize_t count = 0

    builder_map = context.builder_map

    # Alias types for performance.
    t_int32 = _BsonArrowTypes.int32
    t_int64 = _BsonArrowTypes.int64
    t_double = _BsonArrowTypes.double
    t_datetime = _BsonArrowTypes.datetime
    t_oid = _BsonArrowTypes.objectid
    t_string = _BsonArrowTypes.string
    t_bool = _BsonArrowTypes.bool
    t_document = _BsonArrowTypes.document
    t_array = _BsonArrowTypes.array


    # initialize count to current length of builders
    for _, builder in builder_map.items():
        count = len(builder)
        break
    try:
        while True:
            doc = bson_reader_read_safe(stream_reader)
            if doc == NULL:
                break
            if not bson_iter_init(&doc_iter, doc):
                raise InvalidBSON("Could not read BSON document")
            while bson_iter_next(&doc_iter):
                key = bson_iter_key(&doc_iter)
                if arr_value_builder is not None:
                    builder = arr_value_builder
                else:
                    builder = builder_map.get(key)
                if builder is None:
                    builder = builder_map.get(key)
                if builder is None and context.schema is None:
                    # Get the appropriate builder for the current field.
                    value_t = bson_iter_type(&doc_iter)
                    builder_type = _builder_type_map.get(value_t)
                    if builder_type is None:
                        continue

                    # Handle the parameterized builders.
                    if builder_type == DatetimeBuilder and context.tzinfo is not None:
                        arrow_type = timestamp('ms', tz=context.tzinfo)
                        builder = DatetimeBuilder(dtype=arrow_type)

                    elif builder_type == DocumentBuilder:
                        bson_iter_recurse(&doc_iter, &child_iter)
                        struct_dtype = extract_document_dtype(&child_iter, context)
                        builder = DocumentBuilder(struct_dtype, context.tzinfo)
                    elif builder_type == ListBuilder:
                        bson_iter_recurse(&doc_iter, &child_iter)
                        list_dtype = extract_array_dtype(&child_iter, context)
                        list_dtype = list_(list_dtype)
                        builder = ListBuilder(list_dtype, context.tzinfo, value_builder=arr_value_builder)
                    else:
                        builder = builder_type()
                    if arr_value_builder is None:
                        builder_map[key] = builder
                    for _ in range(count):
                        builder.append_null()

                if builder is None:
                    continue

                ftype = builder.type_marker
                value_t = bson_iter_type(&doc_iter)
                if ftype == t_int32:
                    if value_t == BSON_TYPE_INT32:
                        builder.append(bson_iter_int32(&doc_iter))
                    else:
                        builder.append_null()
                elif ftype == t_int64:
                    if (value_t == BSON_TYPE_INT64 or
                            value_t == BSON_TYPE_BOOL or
                            value_t == BSON_TYPE_DOUBLE or
                            value_t == BSON_TYPE_INT32):
                        builder.append(bson_iter_as_int64(&doc_iter))
                    else:
                        builder.append_null()
                elif ftype == t_oid:
                    if value_t == BSON_TYPE_OID:
                        builder.append(<bytes>(<uint8_t*>bson_iter_oid(&doc_iter))[:12])
                    else:
                        builder.append_null()
                elif ftype == t_string:
                    if value_t == BSON_TYPE_UTF8:
                        bson_str = bson_iter_utf8(&doc_iter, &str_len)
                        builder.append(<bytes>(bson_str)[:str_len])
                    elif value_t == BSON_TYPE_DECIMAL128:
                        bson_iter_decimal128(&doc_iter, &dec128)
                        bson_decimal128_to_string(&dec128, decimal128_str)
                        builder.append(<bytes>(decimal128_str))
                    else:
                        builder.append_null()
                elif ftype == t_double:
                    if (value_t == BSON_TYPE_DOUBLE or
                            value_t == BSON_TYPE_BOOL or
                            value_t == BSON_TYPE_INT32 or
                            value_t == BSON_TYPE_INT64):
                        builder.append(bson_iter_as_double(&doc_iter))
                    else:
                        builder.append_null()
                elif ftype == t_datetime:
                    if value_t == BSON_TYPE_DATE_TIME:
                        builder.append(bson_iter_date_time(&doc_iter))
                    else:
                        builder.append_null()
                elif ftype == t_bool:
                    if value_t == BSON_TYPE_BOOL:
                        builder.append(bson_iter_bool(&doc_iter))
                    else:
                        builder.append_null()
                elif ftype == t_document:
                    if value_t == BSON_TYPE_DOCUMENT:
                        bson_iter_document(&doc_iter, &doc_buf_len, &doc_buf)
                        if doc_buf_len <= 0:
                            raise ValueError("Subdocument is invalid")
                        builder.append(<bytes>doc_buf[:doc_buf_len])
                    else:
                        builder.append_null()
                elif ftype == t_array:
                    if value_t == BSON_TYPE_ARRAY:
                        bson_iter_array(&doc_iter, &doc_buf_len, &doc_buf)
                        if doc_buf_len <= 0:
                            raise ValueError("Subarray is invalid")
                        builder.append(<bytes>doc_buf[:doc_buf_len])
                    else:
                        builder.append_null()
                else:
                    raise PyMongoArrowError('unknown ftype {}'.format(ftype))
            count += 1
            for _, builder in builder_map.items():
                if len(builder) != count:
                    # Append null to account for any missing field(s)
                    builder.append_null()
    finally:
        bson_reader_destroy(stream_reader)
        free(decimal128_str)


# Builders

cdef class _ArrayBuilderBase:
    def append_values(self, values):
        for value in values:
            if value is None or value is np.nan:
                self.append_null()
            else:
                self.append(value)


cdef class StringBuilder(_ArrayBuilderBase):
    type_marker = _BsonArrowTypes.string
    cdef:
        shared_ptr[CStringBuilder] builder

    def __cinit__(self, MemoryPool memory_pool=None):
        cdef CMemoryPool* pool = maybe_unbox_memory_pool(memory_pool)
        self.builder.reset(new CStringBuilder(pool))

    cpdef append_null(self):
        self.builder.get().AppendNull()

    def __len__(self):
        return self.builder.get().length()

    cpdef append(self, value):
        self.builder.get().Append(tobytes(value))

    cpdef finish(self):
        cdef shared_ptr[CArray] out
        with nogil:
            self.builder.get().Finish(&out)
        return pyarrow_wrap_array(out)

    cdef shared_ptr[CStringBuilder] unwrap(self):
        return self.builder


cdef class ObjectIdBuilder(_ArrayBuilderBase):
    type_marker = _BsonArrowTypes.objectid
    cdef:
        shared_ptr[CFixedSizeBinaryBuilder] builder

    def __cinit__(self, MemoryPool memory_pool=None):
        cdef shared_ptr[CDataType] dtype = fixed_size_binary(12)
        cdef CMemoryPool* pool = maybe_unbox_memory_pool(memory_pool)
        self.builder.reset(new CFixedSizeBinaryBuilder(dtype, pool))

    cpdef append_null(self):
        self.builder.get().AppendNull()

    def __len__(self):
        return self.builder.get().length()

    cpdef append(self, value):
        self.builder.get().Append(value)

    cpdef finish(self):
        cdef shared_ptr[CArray] out
        with nogil:
            self.builder.get().Finish(&out)
        return pyarrow_wrap_array(out)

    cdef shared_ptr[CFixedSizeBinaryBuilder] unwrap(self):
        return self.builder


cdef class Int32Builder(_ArrayBuilderBase):
    type_marker = _BsonArrowTypes.int32
    cdef:
        shared_ptr[CInt32Builder] builder

    def __cinit__(self, MemoryPool memory_pool=None):
        cdef CMemoryPool* pool = maybe_unbox_memory_pool(memory_pool)
        self.builder.reset(new CInt32Builder(pool))

    cpdef append_null(self):
        self.builder.get().AppendNull()

    def __len__(self):
        return self.builder.get().length()

    cpdef append(self, value):
        self.builder.get().Append(value)

    cpdef finish(self):
        cdef shared_ptr[CArray] out
        with nogil:
            self.builder.get().Finish(&out)
        return pyarrow_wrap_array(out)

    cdef shared_ptr[CInt32Builder] unwrap(self):
        return self.builder


cdef class Int64Builder(_ArrayBuilderBase):
    type_marker = _BsonArrowTypes.int64
    cdef:
        shared_ptr[CInt64Builder] builder

    def __cinit__(self, MemoryPool memory_pool=None):
        cdef CMemoryPool* pool = maybe_unbox_memory_pool(memory_pool)
        self.builder.reset(new CInt64Builder(pool))

    cpdef append_null(self):
        self.builder.get().AppendNull()

    def __len__(self):
        return self.builder.get().length()

    cpdef append(self, value):
        self.builder.get().Append(value)

    cpdef finish(self):
        cdef shared_ptr[CArray] out
        with nogil:
            self.builder.get().Finish(&out)
        return pyarrow_wrap_array(out)

    cdef shared_ptr[CInt64Builder] unwrap(self):
        return self.builder


cdef class DoubleBuilder(_ArrayBuilderBase):
    type_marker = _BsonArrowTypes.double
    cdef:
        shared_ptr[CDoubleBuilder] builder

    def __cinit__(self, MemoryPool memory_pool=None):
        cdef CMemoryPool* pool = maybe_unbox_memory_pool(memory_pool)
        self.builder.reset(new CDoubleBuilder(pool))

    cpdef append_null(self):
        self.builder.get().AppendNull()

    def __len__(self):
        return self.builder.get().length()

    cpdef append(self, value):
        self.builder.get().Append(value)

    cpdef finish(self):
        cdef shared_ptr[CArray] out
        with nogil:
            self.builder.get().Finish(&out)
        return pyarrow_wrap_array(out)

    cdef shared_ptr[CDoubleBuilder] unwrap(self):
        return self.builder


cdef class DatetimeBuilder(_ArrayBuilderBase):
    type_marker = _BsonArrowTypes.datetime
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

    cpdef append_null(self):
        self.builder.get().AppendNull()

    def __len__(self):
        return self.builder.get().length()

    cpdef append(self, value):
        self.builder.get().Append(value)

    cpdef finish(self):
        cdef shared_ptr[CArray] out
        with nogil:
            self.builder.get().Finish(&out)
        return pyarrow_wrap_array(out)

    @property
    def unit(self):
        return self.dtype

    cdef shared_ptr[CTimestampBuilder] unwrap(self):
        return self.builder


cdef class BoolBuilder(_ArrayBuilderBase):
    type_marker = _BsonArrowTypes.bool
    cdef:
        shared_ptr[CBooleanBuilder] builder

    def __cinit__(self, MemoryPool memory_pool=None):
        cdef CMemoryPool* pool = maybe_unbox_memory_pool(memory_pool)
        self.builder.reset(new CBooleanBuilder(pool))

    cpdef append_null(self):
        self.builder.get().AppendNull()

    def __len__(self):
        return self.builder.get().length()

    cpdef append(self, value):
        self.builder.get().Append(<c_bool>value)

    cpdef finish(self):
        cdef shared_ptr[CArray] out
        with nogil:
            self.builder.get().Finish(&out)
        return pyarrow_wrap_array(out)

    cdef shared_ptr[CBooleanBuilder] unwrap(self):
        return self.builder



cdef object get_field_builder(field, tzinfo):
    """"Find the appropriate field builder given a pyarrow field"""
    cdef object field_builder
    cdef DataType field_type
    if isinstance(field, DataType):
        field_type = field
    else:
        field_type = field.type
    if _atypes.is_int32(field_type):
        field_builder = Int32Builder()
    elif _atypes.is_int64(field_type):
        field_builder = Int64Builder()
    elif _atypes.is_float64(field_type):
        field_builder = DoubleBuilder()
    elif _atypes.is_timestamp(field_type):
        if tzinfo and field_type.tz is None:
            field_type = timestamp(field_type.unit, tz=tzinfo)
        field_builder = DatetimeBuilder(field_type)
    elif _atypes.is_string(field_type):
        field_builder = StringBuilder()
    elif _atypes.is_boolean(field_type):
        field_builder = BoolBuilder()
    elif _atypes.is_struct(field_type):
        field_builder = DocumentBuilder(field_type, tzinfo)
    elif _atypes.is_list(field_type):
        field_builder = ListBuilder(field_type, tzinfo)
    elif getattr(field_type, '_type_marker') == _BsonArrowTypes.objectid:
        field_builder = ObjectIdBuilder()
    elif getattr(field_type, '_type_marker') == _BsonArrowTypes.decimal128_str:
        field_builder = StringBuilder()
    else:
        field_builder = StringBuilder()
    return field_builder


cdef class DocumentBuilder(_ArrayBuilderBase):
    type_marker = _BsonArrowTypes.document

    cdef:
        shared_ptr[CStructBuilder] builder
        object dtype
        object context

    def __cinit__(self, StructType dtype, tzinfo=None, MemoryPool memory_pool=None):
        cdef StringBuilder field_builder
        cdef vector[shared_ptr[CArrayBuilder]] c_field_builders
        cdef CMemoryPool* pool = maybe_unbox_memory_pool(memory_pool)

        self.dtype = dtype
        if not _atypes.is_struct(dtype):
            raise ValueError("dtype must be a struct()")

        self.context = context = PyMongoArrowContext(None, {})
        context.tzinfo = tzinfo
        builder_map = context.builder_map

        for field in dtype:
            field_builder = <StringBuilder>get_field_builder(field, tzinfo)
            builder_map[field.name.encode('utf-8')] = field_builder
            c_field_builders.push_back(<shared_ptr[CArrayBuilder]>field_builder.builder)

        self.builder.reset(new CStructBuilder(pyarrow_unwrap_data_type(dtype), pool, c_field_builders))

    @property
    def dtype(self):
        return self.dtype

    cpdef append_null(self):
        self.builder.get().AppendNull()

    def __len__(self):
        return self.builder.get().length()

    cpdef append(self, value):
        if not isinstance(value, bytes):
            value = bson.encode(value)
        # Populate the child builders.
        process_bson_stream(value, self.context)
        # Append an element to the Struct. "All child-builders' Append method
        # must be called independently to maintain data-structure consistency."
        # Pass "true" for is_valid.
        self.builder.get().Append(True)

    cpdef finish(self):
        cdef shared_ptr[CArray] out
        with nogil:
            self.builder.get().Finish(&out)
        return pyarrow_wrap_array(out)

    cdef shared_ptr[CStructBuilder] unwrap(self):
        return self.builder

cdef class ListBuilder(_ArrayBuilderBase):
    type_marker = _BsonArrowTypes.array

    cdef:
        shared_ptr[CListBuilder] builder
        _ArrayBuilderBase child_builder
        object dtype
        object context

    def __cinit__(self, DataType dtype, tzinfo=None, MemoryPool memory_pool=None, value_builder=None):
        cdef StringBuilder field_builder
        cdef CMemoryPool* pool = maybe_unbox_memory_pool(memory_pool)
        cdef shared_ptr[CArrayBuilder] grandchild_builder
        self.dtype = dtype
        if not _atypes.is_list(dtype):
            raise ValueError("dtype must be a list_()")
        self.context = context = PyMongoArrowContext(None, {})
        self.context.tzinfo = tzinfo
        field_builder = <StringBuilder>get_field_builder(self.dtype.value_type, tzinfo)
        grandchild_builder = <shared_ptr[CArrayBuilder]>field_builder.builder
        self.child_builder = field_builder
        self.builder.reset(new CListBuilder(pool, grandchild_builder, pyarrow_unwrap_data_type(dtype)))


    @property
    def dtype(self):
        return self.dtype

    cpdef append_null(self):
        self.builder.get().AppendNull()

    def __len__(self):
        return self.builder.get().length()

    cpdef append(self, value):
        if not isinstance(value, bytes):
            value = bson.encode(value)
        # Append an element to the array.
        # arr_value_builder will be appended to by process_bson_stream.
        self.builder.get().Append(True)
        process_bson_stream(value, self.context, arr_value_builder=self.child_builder)


    cpdef finish(self):
        cdef shared_ptr[CArray] out
        with nogil:
            self.builder.get().Finish(&out)
        return pyarrow_wrap_array(out)

    cdef shared_ptr[CListBuilder] unwrap(self):
        return self.builder
