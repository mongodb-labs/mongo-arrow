# Copyright 2025-present MongoDB, Inc.
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
from test import client_context

import pytest

try:
    import pandas as pd  # noqa: F401

    pytest_plugins = [
        "pandas.tests.extension.conftest",
    ]
except ImportError:
    pass


@pytest.fixture(autouse=True, scope="session")
def client():
    client_context.init()
    yield
    if client_context.client:
        client_context.client.close()
