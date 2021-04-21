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


cdef const bson_t* bson_reader_read_safe(bson_reader_t* stream_reader) except? NULL:
    cdef cbool reached_eof = False
    cdef const bson_t* doc = bson_reader_read(stream_reader, &reached_eof)
    if doc == NULL and reached_eof == False:
        raise InvalidBSON("Could not read BSON document stream")
    return doc


def process_bson_stream(bson_stream, context):
    cdef const uint8_t* docstream = <const uint8_t *>bson_stream
    cdef size_t length = <size_t>PyBytes_Size(bson_stream)
    cdef bson_reader_t* stream_reader = bson_reader_new_from_data(docstream, length)
    cdef const bson_t * doc = NULL
    cdef bson_iter_t doc_iter
    cdef const char* key
    cdef bson_type_t value_t
    cdef Py_ssize_t count = 0

    # Localize types for better performance.
    t_int32 = _BsonArrowTypes.int32
    t_int64 = _BsonArrowTypes.int64
    t_double = _BsonArrowTypes.double
    t_datetime = _BsonArrowTypes.datetime
    builder_map = context.builder_map

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
                builder = builder_map.get(key)
                if builder is not None:
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
                    else:
                        raise PyMongoArrowError('unknown ftype {}'.format(ftype))
            count += 1
            for _, builder in builder_map.items():
                if len(builder) != count:
                    # Append null to account for any missing field(s)
                    builder.append_null()
    finally:
        bson_reader_destroy(stream_reader)
