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
"""Add PyMongoArrow APIs to PyMongo."""

__all__ = ["patch_all"]


def patch_all():
    """Patch all PyMongoArrow methods into PyMongo.

    Calling this method equips the :class:`pymongo.collection.Collection`
    classes returned by PyMongo with PyMongoArrow's API methods. When using a
    patched method, users can omit the first argument which is passed
    implicitly. For example::

       # Example of direct usage
       df = find_pandas_all(coll.db.test, {'amount': {'$gte': 20}}, schema=schema)

       # Example of patched usage
       df = coll.db.test.find_pandas_all({'amount': {'$gte': 20}}, schema=schema)
    """
    import pymongoarrow.api as api_module
    from pymongo.collection import Collection

    api_methods = api_module._PATCH_METHODS
    for method_name in api_methods:
        method = getattr(api_module, method_name)
        setattr(Collection, method.__name__, method)
