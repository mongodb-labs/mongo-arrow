# distutils: language=c++
# cython: language_level=3

from libcpp cimport bool as cbool

from pymongoarrow.libbson cimport *


cdef list _docs_from_bson_stream(const uint8_t *docstream, size_t length):
    cdef bson_reader_t *stream_reader = bson_reader_new_from_data(docstream, length)

    cdef cbool reached_eof = False
    cdef const bson_t *doc = bson_reader_read(stream_reader, &reached_eof)
    cdef bson_iter_t doc_iter

    documents = []

    while doc != NULL:
        if not bson_iter_init(&doc_iter, doc):
            raise RuntimeError
        document = {}
        while bson_iter_next(&doc_iter):
            key = bson_iter_key(&doc_iter)
            value = bson_iter_int32(&doc_iter)
            document[key.decode('utf-8')] = value
        documents.append(document)
        doc = bson_reader_read(stream_reader, &reached_eof)

    return documents


def docs_from_bson_stream(docstream):
    return _docs_from_bson_stream(docstream, len(docstream))
