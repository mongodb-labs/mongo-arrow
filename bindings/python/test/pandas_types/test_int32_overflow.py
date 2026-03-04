import pyarrow as pa
import pytest
from pymongo import MongoClient

from pymongoarrow.api import Schema
from pymongoarrow.monkey import patch_all

try:
    import pandas as pd  # noqa: F401
except ImportError:
    pytest.skip("skipping pandas tests", allow_module_level=True)

patch_all()


def test_aggregate_pandas_all_schema_inference_int32_avoids_overflow():
    client = MongoClient()
    db = client["pymongoarrow_int32_overflow_test_db"]
    coll = db["mixed_ints"]
    coll.drop()

    # One small int and one very large int for the same field.
    # Small value fits in Int32; large value does not.
    coll.insert_many(
        [
            {"_id": 1, "value": 1},
            {"_id": 2, "value": 2**40},  # much larger than Int32 max
        ]
    )

    pipeline = [
        {"$project": {"_id": 1, "value": 1}},
        {"$sort": {"_id": 1}},  # bias inference to see small value first
    ]

    df = coll.aggregate_pandas_all(pipeline)

    assert len(df) == 2
    assert df["value"].max() == 2**40
    client.close()


def test_aggregate_pandas_all_explicit_int64_schema_avoids_overflow():
    client = MongoClient()
    db = client["pymongoarrow_int32_overflow_test_db"]
    coll = db["mixed_ints"]

    pipeline = [
        {"$project": {"_id": 1, "value": 1}},
        {"$sort": {"_id": 1}},
    ]

    # Explicitly choose a 64-bit integer type for the problematic field.
    schema = Schema(
        {
            "_id": pa.int64(),
            "value": pa.int64(),
        }
    )

    df = coll.aggregate_pandas_all(pipeline, schema=schema)

    assert len(df) == 2
    assert df["value"].max() == 2**40
    client.close()
