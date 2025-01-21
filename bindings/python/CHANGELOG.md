
# Changelog

---

# Changes in Version 1.6.2 (2024/12/18)

- Fix macos wheel build for Python 3.9, which prevented the release of 1.6.1.

# Changes in Version 1.6.1 (2024/12/18)

- Fix handling of missing document fields.

# Changes in Version 1.6.0 (YYYY/11/04)

- Add support for PyArrow 18.0 and Python 3.13.
- Drop support for Python 3.8.
- Fix support for nulls and extension types in nested data.
- Add support for PyArrow's `null` type.
- Prevent segmentation faults by checking the status of all calls to the PyArrow C APIs.

# Changes in Version 1.5.2 (2024/09/23)

- Fix support for PyMongo 4.9.
- Fix building from source by pinning setuptools.

# Changes in Version 1.5.1 (2024/08/12)

- Remove upper bound version pin on `packaging`.

# Changes in Version 1.5.0 (2024/08/07)

- Support for PyArrow 17.0
- Support for nested ObjectIDs in polars conversion

# Changes in Version 1.4.0 (2024/06/05)

- Support for PyArrow 16.0
- Migrated documentation from [readthedocs](https://mongo-arrow.readthedocs.io/en/latest/index.html)
to [MongoDB Docs](https://www.mongodb.com/docs/languages/python/pymongo-arrow-driver/current/)
- Added a top-level Contributing guide
- Added an optional bool flag to the write function to skip writing null fields

# Changes in Version 1.3.0 (2024/02/06)

- Support for Polars
- Support for PyArrow.DataTypes: large_list, large_string, date32,
  date64

# Changes in Version 1.2.0 (2023/12/12)

- Support for PyArrow 14.0.
- Support for Python 3.12.

# Changes in Version 1.1.0 (2023/10/18)

- Support for PyArrow 13.0.
- Revert bug fix for nested extension objects in auto schema, since it
  caused a performance regression.

# Changes in Version 1.0.2 (2023/07/17)

- Bug fix for projection on nested fields.
- Bug fix for nested extension objects in auto schema.

# Changes in Version 1.0.1 (2023/06/22)

> [!NOTE]
> The 1.0.0 release had an incorrect changelog.

- Support BSON binary type.
- Support BSON Decimal128 type.
- Support Pandas 2.0 and Pandas extension types.
- Support PyArrow 12.0.

# Changes in Version 1.0.0 (2023/06/21)

# Changes in Version 0.7.0 (2023/01/30)

- Added support for BSON Embedded Document type.
- Added support for BSON Array type.
- Support PyArrow 11.0.

# Changes in Version 0.6.3 (2022/12/14)

- Added wheels for Linux AArch64 and Python 3.11.
- Fixed handling of time zones in schema auto-discovery.

# Changes in Version 0.6.2 (2022/11/16)

> [!NOTE]
> We did not publish 0.6.0 or 0.6.1 due to technical difficulties.

- Fixed `ImportError` on Windows by building `libbson` in \"Release\"
  mode.
- Support PyArrow 10.0.

# Changes in Version 0.5.1 (2022/08/31)

- Fixed auto-discovery of schemas for aggregation and `numpy` methods.
- Added documentation for auto-discovery of schemas.

# Changes in Version 0.5.0 (2022/08/18)

- Support auto-discovery of schemas in `find/aggregate_*_all` methods.
  If the schema is not given, it will be inferred using the first
  document in the result set.
- Support PyArrow 9.0.
- Improve error message for lib ImportError.

# Changes in Version 0.4.0 (2022/04/26)

- Support for `~bson.decimal128.Decimal128`{.interpreted-text
  role="class"} type.
- Support for macOS arm64 architecture on Python 3.9+.
- Support for writing tabular datasets (materialized as PyArrow
  Tables, Pandas DataFrames, or NumPy arrays) to MongoDB using the
  `~pymongoarrow.api.write`{.interpreted-text role="meth"} function.
  See the `quickstart`{.interpreted-text role="doc"} guide for more
  info.

# Changes in Version 0.3.0 (2022/03/02)

- Support for `PyArrow` 7.0.
- Support for `~bson.objectid.ObjectId`{.interpreted-text
  role="class"} type.
- Improve error message when schema contains an unsupported type.
- Add support for BSON string type.
- Add support for BSON boolean type.
- Upgraded to bundle
  [libbson](http://mongoc.org/libbson/current/index.html) 1.21.1. If
  installing from source, the minimum supported `libbson` version is
  now 1.21.0.
- Dropped Python 3.6 support (it was dropped in `PyArrow` 7.0).

# Changes in Version 0.2.0 (2022/01/06)

- Support for PyMongo 4.0.
- Support for Python 3.10.
- Support for Windows.
- `~pymongoarrow.api.find_arrow_all`{.interpreted-text role="meth"}
  now accepts a user-provided `projection`.
- `~pymongoarrow.api.find_arrow_all`{.interpreted-text role="meth"}
  now accepts a `session` object.

> [!NOTE]
> The supported version of `pyarrow` is now `>=6,<6.1`.

# Changes in Version 0.1.1 (2021/04/27)

- Fixed a bug that caused Linux wheels to be created without the
  appropriate `manylinux` platform tags.

# Changes in Version 0.1.0 (2021/04/26)

- Support for efficiently converting find and aggregate query result
  sets into Arrow/Pandas/Numpy data structures.
- Support for patching PyMongo\'s APIs using
  `~pymongoarrow.monkey.patch_all`{.interpreted-text role="meth"}
- Support for loading the following [BSON
  types](http://bsonspec.org/spec.html):
    - 64-bit binary floating point
    - 32-bit integer
    - 64-bit integer
    - Timestamp
