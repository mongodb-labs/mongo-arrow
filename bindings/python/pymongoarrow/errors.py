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

"""Exceptions raised by PyMongoArrow."""

from bson.errors import InvalidBSON  # noqa: F401


class PyMongoArrowError(Exception):
    """Base class for all PyMongoArrow exceptions."""

    pass


class ArrowWriteError(PyMongoArrowError):
    """Error raised when we encounter an exception writing into MongoDB"""

    def __init__(self, details):
        self._details = details

    @property
    def details(self):
        """Details for the error.

        It is a dictionary of key-value pairs giving diagnostic information about what went wrong.
        To see the entire dictionary simply use `print(awe.details)`.

        Details will have the following format:
        {
            'writeErrors': [...],
            'writeConcernErrors': [...],
            'nInserted': ...,
        }

        If the error was caused by a PyMongo exception, then you can access that exception using the
        ``__cause__`` attribute.
        """
        return self._details
