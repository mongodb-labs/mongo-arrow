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

libbson_version = bson_get_version().decode('utf-8')


def datetime_to_int64(dtm, data_type):
    # TODO: rewrite as a cdef which directly accesses data_type as a CTimestampType instance
    # TODO: make this function aware of datatype.timezone()
    total_seconds = int((dtm - datetime.datetime(1970, 1, 1)).total_seconds())
    total_microseconds = int(total_seconds) * 10**6 + dtm.microsecond

    if data_type.unit == 's':
        factor = 1.
    elif data_type.unit == 'ms':
        factor = 10. ** 3
    elif data_type.unit == 'us':
        factor = 10. ** 6
    elif data_type.unit == 'ns':
        factor = 10. ** 9
    else:
        raise ValueError('Unsupported timestamp unit {}'.format(
            data_type.unit))

    int64_t = int(total_microseconds * factor / (10. ** 6))
    return int64_t
