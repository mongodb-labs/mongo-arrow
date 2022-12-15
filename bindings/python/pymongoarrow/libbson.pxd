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
# cython: language_level=3
# distutils: language=c
from libc.stdint cimport int32_t, int64_t, uint8_t, uint32_t, uint64_t


# libbson type wrappings
# wrappings are not defined for the following types:
# - bson_json_reader_t
# - bson_md5_t (deprecated)
# - bson_string_t
# - bson_subtype_t
# - bson_unichar_t
# - bson_value_t
# - bson_visitor_t
# - bson_writer_t
cdef extern from "<bson/bson.h>":
    ctypedef struct bson_t:
        uint32_t flags
        uint32_t len
        uint8_t padding[120]

    ctypedef struct bson_context_t:
        pass

    ctypedef struct bson_decimal128_t:
        pass

    ctypedef struct bson_error_t:
        uint32_t domain
        uint32_t code
        char message[504]

    ctypedef struct bson_iter_t:
        pass

    ctypedef struct bson_oid_t:
        uint8_t bytes[12]

    ctypedef struct bson_reader_t:
        pass

    ctypedef enum bson_type_t:
        BSON_TYPE_EOD,
        BSON_TYPE_DOUBLE,
        BSON_TYPE_UTF8,
        BSON_TYPE_DOCUMENT,
        BSON_TYPE_ARRAY,
        BSON_TYPE_BINARY,
        BSON_TYPE_UNDEFINED,
        BSON_TYPE_OID,
        BSON_TYPE_BOOL,
        BSON_TYPE_DATE_TIME,
        BSON_TYPE_NULL,
        BSON_TYPE_REGEX,
        BSON_TYPE_DBPOINTER,
        BSON_TYPE_CODE,
        BSON_TYPE_SYMBOL,
        BSON_TYPE_CODEWSCOPE,
        BSON_TYPE_INT32,
        BSON_TYPE_TIMESTAMP,
        BSON_TYPE_INT64,
        BSON_TYPE_DECIMAL128,
        BSON_TYPE_MAXKEY,
        BSON_TYPE_MINKEY


# bson_t API
cdef extern from "<bson/bson.h>":
    void bson_destroy(bson_t *bson)

    const uint8_t * bson_get_data(const bson_t *bson)

    bint bson_has_field(const bson_t *bson, const char *key)

    bint bson_init_static(bson_t *b, const uint8_t *data, size_t length)

    char * bson_as_json(const bson_t *bson, size_t *length)


# bson_iter_t API
cdef extern from "<bson/bson.h>":
    bint bson_iter_init(bson_iter_t *iter, const bson_t *bson)

    bint bson_iter_init_from_data(bson_iter_t *iter, const uint8_t *data, size_t length)

    bint bson_iter_next(bson_iter_t *iter)

    const char * bson_iter_key(const bson_iter_t *iter)

    bson_type_t bson_iter_type(const bson_iter_t *iter)

    bint bson_iter_bool(const bson_iter_t *iter)

    int64_t bson_iter_date_time(const bson_iter_t *iter)

    const bson_oid_t * bson_iter_oid(const bson_iter_t *iter)

    # TODO: add decimal128

    double bson_iter_double(const bson_iter_t *iter)

    double bson_iter_as_double(const bson_iter_t *iter)

    int32_t bson_iter_int32(const bson_iter_t *iter)

    int64_t bson_iter_int64(const bson_iter_t *iter)

    int64_t bson_iter_as_int64(const bson_iter_t *iter)

    const char * bson_iter_utf8(const bson_iter_t *iter, uint32_t *length)

    bint bson_iter_decimal128 (const bson_iter_t *iter, bson_decimal128_t *dec)

    bint  bson_iter_recurse (const bson_iter_t *iter, # IN
                             bson_iter_t *child)  # OUT

    void bson_iter_document (const bson_iter_t *iter,  # IN
                    uint32_t *document_len,  #  OUT
                    const uint8_t **document) # OUT

    void bson_iter_array (const bson_iter_t *iter, # IN
                 uint32_t *array_len,     # OUT
                 const uint8_t **array)


# bson_reader_t API
cdef extern from "<bson/bson.h>":
    bson_reader_t * bson_reader_new_from_data(const uint8_t *data, size_t length)

    const bson_t * bson_reader_read(bson_reader_t *reader, bint *reached_eof)

    void bson_reader_destroy(bson_reader_t *reader)

# bson_decimal128 API
cdef extern from "<bson/bson.h>":
    cdef int32_t BSON_DECIMAL128_STRING
    void bson_decimal128_to_string (const bson_decimal128_t *dec, char *str)

# runtime version checking API
cdef extern from "<bson/bson.h>":
    const char * bson_get_version()
