# Copyright 2022-present MongoDB, Inc.
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

"""Results returned by PyMongoArrow."""


class ArrowWriteResult:
    def __init__(self, result_dict):
        self._result = result_dict

    def __repr__(self):
        return repr(self._result)

    @property
    def inserted_count(self):
        return self._result.get("insertedCount", 0)

    @property
    def raw_result(self):
        return self._result
