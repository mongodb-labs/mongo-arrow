# Copyright 2026-present MongoDB, Inc.
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

from __future__ import annotations

try:
    import polars as pl
except ImportError:
    pl = None


class PolarsExtensionBase(pl.datatypes.BaseExtension):
    def __eq__(self, other):
        return (
            isinstance(other, self.__class__)
            and self.ext_name() == other.ext_name()
            and self.ext_storage() == other.ext_storage()
        )


class PolarsBinary(PolarsExtensionBase):
    def __init__(self) -> None:
        super().__init__(name="pymongoarrow.binary", storage=pl.Binary)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}"

    def _string_repr(self) -> str:
        return "binary"


class PolarsObjectId(PolarsExtensionBase):
    def __init__(self) -> None:
        super().__init__(name="pymongoarrow.objectid", storage=pl.Binary)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}"

    def _string_repr(self) -> str:
        return "objectid"


class PolarsCode(PolarsExtensionBase):
    def __init__(self) -> None:
        super().__init__(name="pymongoarrow.code", storage=pl.String)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}"

    def _string_repr(self) -> str:
        return "code"


class PolarsDecimal128(PolarsExtensionBase):
    def __init__(self) -> None:
        super().__init__(name="pymongoarrow.decimal128", storage=pl.Binary)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}"

    def _string_repr(self) -> str:
        return "decimal128"
