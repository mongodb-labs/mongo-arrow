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
from pyarrow.lib cimport *

import numpy as np


cdef class Int32Builder(_Weakrefable):
    cdef:
        shared_ptr[CInt32Builder] builder

    def __cinit__(self, MemoryPool memory_pool=None):
        cdef CMemoryPool* pool = maybe_unbox_memory_pool(memory_pool)
        self.builder.reset(new CInt32Builder(pool))

    def append(self, value):
        if value is None or value is np.nan:
            self.builder.get().AppendNull()
        elif isinstance(value, int):
            self.builder.get().Append(value)
        else:
            raise TypeError('Int32Builder only accepts integer objects')

    def append_values(self, values):
        for value in values:
            self.append(value)

    def finish(self):
        cdef shared_ptr[CArray] out
        with nogil:
            self.builder.get().Finish(&out)
        return pyarrow_wrap_array(out)

    @property
    def null_count(self):
        return self.builder.get().null_count()

    def __len__(self):
        return self.builder.get().length()

    cdef shared_ptr[CInt32Builder] unwrap(self):
        return self.builder


cdef class Int64Builder(_Weakrefable):
    cdef:
        shared_ptr[CInt64Builder] builder

    def __cinit__(self, MemoryPool memory_pool=None):
        cdef CMemoryPool* pool = maybe_unbox_memory_pool(memory_pool)
        self.builder.reset(new CInt64Builder(pool))

    def append(self, value):
        if value is None or value is np.nan:
            self.builder.get().AppendNull()
        elif isinstance(value, int):
            self.builder.get().Append(value)
        else:
            raise TypeError('Int64Builder only accepts integer objects')

    def append_values(self, values):
        for value in values:
            self.append(value)

    def finish(self):
        cdef shared_ptr[CArray] out
        with nogil:
            self.builder.get().Finish(&out)
        return pyarrow_wrap_array(out)

    @property
    def null_count(self):
        return self.builder.get().null_count()

    def __len__(self):
        return self.builder.get().length()

    cdef shared_ptr[CInt64Builder] unwrap(self):
        return self.builder
