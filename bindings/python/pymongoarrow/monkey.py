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
from pymongoarrow.api import (
    aggregate_arrow_all, find_arrow_all)


__all__ = [
    'patch_all'
]


def patch_all():
    for method in (aggregate_arrow_all, find_arrow_all):
        if not hasattr(method, "__target__"):
            raise ValueError(
                "cannot patch method with unset '__target__' attribute")

        target_spec = method.__target__
        target_module_name, cls_name = target_spec.rsplit('.', maxsplit=1)
        target_module = __import__(target_module_name, fromlist=[cls_name])
        target = getattr(target_module, cls_name)
        setattr(target, method.__name__, method)
