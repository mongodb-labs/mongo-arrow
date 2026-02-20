import pytest

from pymongo import MongoClient
from pymongoarrow.monkey import patch_all
from pymongoarrow.api import Schema
import pyarrow as pa

# Enable aggregate_pandas_all, etc., on Collection instances.
patch_all()

@pytest.mark.xfail(
    raises=OverflowError,
    reason="INTPYTHON-870: schema inference chooses Int32 for mixed ints and overflows",
)
def test_aggregate_pandas_all_schema_inference_int32_overflow():
    client = MongoClient()  # adapt to use test client fixture if desired
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

    # Without an explicit schema, inference is expected to choose Int32
    # and then overflow when it sees the large value.
    coll.aggregate_pandas_all(pipeline)

def test_aggregate_pandas_all_explicit_int64_schema_avoids_overflow():
    client = MongoClient()  # adapt to use test client fixture if desired
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

    # Sanity checks: we got both rows and the large value survived.
    assert len(df) == 2
    assert df["value"].max() == 2**40
