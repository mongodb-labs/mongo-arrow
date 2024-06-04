
# Changelog

---

# Changes in Version 1.4.0

-   Support for PyArrow 16.0
-   Migrated documentation from [readthedocs](https://mongo-arrow.readthedocs.io/en/latest/index.html)
to [MongoDB Docs](https://www.mongodb.com/docs/languages/python/pymongo-arrow-driver/current/)
-   Added a top-level Contributing guide
-   Added an optional bool flag to the write function to skip writing null fields

# Changes in Version 1.3.0

-   Support for Polars
-   Support for PyArrow.DataTypes: large_list, large_string, date32,
    date64

# Changes in Version 1.2.0

-   Support for PyArrow 14.0.
-   Support for Python 3.12.

# Changes in Version 1.1.0

-   Support for PyArrow 13.0.
-   Revert bug fix for nested extension objects in auto schema, since it
    caused a performance regression.

# Changes in Version 1.0.2

-   Bug fix for projection on nested fields.
-   Bug fix for nested extension objects in auto schema.

# Changes in Version 1.0.1

Note: The 1.0.0 release had an incorrect changelog.

-   Support BSON binary type.
-   Support BSON Decimal128 type.
-   Support Pandas 2.0 and Pandas extension types.
-   Support PyArrow 12.0.

# Changes in Version 0.7.0

-   Added support for BSON Embedded Document type.
-   Added support for BSON Array type.
-   Support PyArrow 11.0.

# Changes in Version 0.6.3

-   Added wheels for Linux AArch64 and Python 3.11.
-   Fixed handling of time zones in schema auto-discovery.

# Changes in Version 0.6.2

Note: We did not publish 0.6.0 or 0.6.1 due to technical difficulties.

-   Fixed `ImportError` on Windows by building `libbson` in \"Release\"
    mode.
-   Support PyArrow 10.0.

# Changes in Version 0.5.1

-   Fixed auto-discovery of schemas for aggregation and `numpy` methods.
-   Added documentation for auto-discovery of schemas.

# Changes in Version 0.5.0

-   Support auto-discovery of schemas in `find/aggregate_*_all` methods.
    If the schema is not given, it will be inferred using the first
    document in the result set.
-   Support PyArrow 9.0.
-   Improve error message for lib ImportError.

# Changes in Version 0.4.0

-   Support for `~bson.decimal128.Decimal128`{.interpreted-text
    role="class"} type.
-   Support for macOS arm64 architecture on Python 3.9+.
-   Support for writing tabular datasets (materialized as PyArrow
    Tables, Pandas DataFrames, or NumPy arrays) to MongoDB using the
    `~pymongoarrow.api.write`{.interpreted-text role="meth"} function.
    See the `quickstart`{.interpreted-text role="doc"} guide for more
    info.

# Changes in Version 0.3.0

-   Support for `PyArrow` 7.0.
-   Support for `~bson.objectid.ObjectId`{.interpreted-text
    role="class"} type.
-   Improve error message when schema contains an unsupported type.
-   Add support for BSON string type.
-   Add support for BSON boolean type.
-   Upgraded to bundle
    [libbson](http://mongoc.org/libbson/current/index.html) 1.21.1. If
    installing from source, the minimum supported `libbson` version is
    now 1.21.0.
-   Dropped Python 3.6 support (it was dropped in `PyArrow` 7.0).

# Changes in Version 0.2.0

-   Support for PyMongo 4.0.
-   Support for Python 3.10.
-   Support for Windows.
-   `~pymongoarrow.api.find_arrow_all`{.interpreted-text role="meth"}
    now accepts a user-provided `projection`.
-   `~pymongoarrow.api.find_arrow_all`{.interpreted-text role="meth"}
    now accepts a `session` object.
-   Note: The supported version of `pyarrow` is now `>=6,<6.1`.

# Changes in Version 0.1.1

-   Fixed a bug that caused Linux wheels to be created without the
    appropriate `manylinux` platform tags.

# Changes in Version 0.1.0

-   Support for efficiently converting find and aggregate query result
    sets into Arrow/Pandas/Numpy data structures.
-   Support for patching PyMongo\'s APIs using
    `~pymongoarrow.monkey.patch_all`{.interpreted-text role="meth"}
-   Support for loading the following [BSON
    types](http://bsonspec.org/spec.html):
    -   64-bit binary floating point
    -   32-bit integer
    -   64-bit integer
    -   Timestamp
