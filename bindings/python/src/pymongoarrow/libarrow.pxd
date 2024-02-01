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
# distutils: language=c++
from libcpp.vector cimport vector
from libc.stdint cimport int32_t, uint8_t
from pyarrow.lib cimport *
from pyarrow.includes.libarrow cimport (CStatus, CMemoryPool)  # noqa: E211


# libarrow type wrappings

cdef extern from "arrow/builder.h" namespace "arrow" nogil:

    cdef cppclass CFixedSizeBinaryBuilder" arrow::FixedSizeBinaryBuilder"(CArrayBuilder):
        CFixedSizeBinaryBuilder(shared_ptr[CDataType], CMemoryPool* pool)
        CStatus Append(const uint8_t* value)

    cdef cppclass CStructBuilder" arrow::StructBuilder"(CArrayBuilder):
        CStructBuilder(shared_ptr[CDataType], CMemoryPool* pool,
                       vector[shared_ptr[CArrayBuilder]] field_builders)
        CStatus Append(uint8_t is_valid)
        CArrayBuilder* field_builder(int i)
        int32_t num_fields()
        shared_ptr[CDataType] type()

    cdef cppclass CListBuilder" arrow::ListBuilder"(CArrayBuilder):
        CListBuilder(CMemoryPool* pool,
                       shared_ptr[CArrayBuilder] value_builder, shared_ptr[CDataType] dtype)
        CStatus Append(uint8_t is_valid)
        CArrayBuilder* value_builder(int i)
        int32_t num_values()
        shared_ptr[CDataType] type()


cdef extern from "arrow/type_fwd.h" namespace "arrow" nogil:
    shared_ptr[CDataType] fixed_size_binary(int32_t byte_width)
