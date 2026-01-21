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
    import pandas as pd

    pytest_plugins = [
        "pandas.tests.extension.conftest",
    ]
except ImportError:
    pass


# Inline from pandas/conftest.py
@pytest.fixture(params=[True, False])
def using_nan_is_na(request):
    opt = request.param
    with pd.option_context("future.distinguish_nan_and_na", not opt):
        yield opt


@pytest.fixture(autouse=True, scope="session")
def client():
    client_context.init()
    yield
    if client_context.client:
        client_context.client.close()
