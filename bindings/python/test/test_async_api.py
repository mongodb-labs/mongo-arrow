from collections.abc import AsyncIterator
from datetime import UTC, datetime

import pandas as pd
import polars as pl
import pyarrow as pa
import pytest
from pymongo import AsyncMongoClient
from pymongo.asynchronous.collection import AsyncCollection

from pymongoarrow.async_api import (
    aggregate_arrow_all,
    aggregate_numpy_all,
    aggregate_pandas_all,
    aggregate_polars_all,
    find_arrow_all,
    find_numpy_all,
    find_pandas_all,
    find_polars_all,
    write,
)
from pymongoarrow.result import ArrowWriteResult


@pytest.fixture
async def collection() -> AsyncIterator[AsyncCollection]:
    async with AsyncMongoClient() as client:
        coll = client.test.test_collection
        await coll.insert_one({"_id": 123, "foo": "bar"})
        yield coll
        await coll.drop()


async def test_find_polars_all(collection: AsyncCollection) -> None:
    df = await find_polars_all(collection, {}, schema=None)
    assert isinstance(df, pl.DataFrame)
    assert not df.is_empty()


async def test_find_arrow_all(collection: AsyncCollection) -> None:
    df = await find_arrow_all(collection, {}, schema=None)
    assert isinstance(df, pa.Table)
    assert df


async def test_find_pandas_all(collection: AsyncCollection) -> None:
    df = await find_pandas_all(collection, {}, schema=None)
    assert isinstance(df, pd.DataFrame)
    assert not df.empty


async def test_find_numpy_all(collection: AsyncCollection) -> None:
    df = await find_numpy_all(collection, {}, schema=None)
    assert isinstance(df, dict)
    assert df


async def test_aggregate_polars_all(collection: AsyncCollection) -> None:
    df = await aggregate_polars_all(collection, [], schema=None)
    assert isinstance(df, pl.DataFrame)
    assert not df.is_empty()


async def test_aggregate_arrow_all(collection: AsyncCollection) -> None:
    df = await aggregate_arrow_all(collection, [], schema=None)
    assert isinstance(df, pa.Table)
    assert df


async def test_aggregate_pandas_all(collection: AsyncCollection) -> None:
    df = await aggregate_pandas_all(collection, [], schema=None)
    assert isinstance(df, pd.DataFrame)
    assert not df.empty


async def test_aggregate_numpy_all(collection: AsyncCollection) -> None:
    df = await aggregate_numpy_all(collection, [], schema=None)
    assert isinstance(df, dict)
    assert df


async def test_write(collection: AsyncCollection) -> None:
    df = pl.DataFrame(  # NOTE: Copied from https://docs.pola.rs/user-guide/getting-started/#installing-polars
        {
            "name": ["Alice Archer", "Ben Brown", "Chloe Cooper", "Daniel Donovan"],
            "birthdate": [
                datetime.now(UTC),
                datetime.now(UTC),
                datetime.now(UTC),
                datetime.now(UTC),
            ],
            "weight": [57.9, 72.5, 53.6, 83.1],
            "height": [1.56, 1.77, 1.65, 1.75],
        }
    )
    write_result = await write(collection, df)
    assert isinstance(write_result, ArrowWriteResult)
    assert write_result.inserted_count
