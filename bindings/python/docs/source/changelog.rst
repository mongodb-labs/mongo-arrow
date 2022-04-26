Changelog
=========

Changes in Version 0.4.0
------------------------
- Support for :class:`~bson.decimal128.Decimal128` type.
- Support for macOS arm64 architecture on Python 3.9+.
- Support for writing tabular datasets (materialized as
  PyArrow Tables, Pandas DataFrames, or NumPy arrays) to MongoDB.
  See the :doc:`quickstart` guide for more info.

Changes in Version 0.3.0
------------------------
- Support for `PyArrow` 7.0.
- Support for :class:`~bson.objectid.ObjectId` type.
- Improve error message when schema contains an unsupported type.
- Add support for BSON string type.
- Add support for BSON boolean type.
- Upgraded to bundle `libbson <http://mongoc.org/libbson/current/index.html>`_ 1.21.1. If installing from source, the minimum supported ``libbson`` version is now 1.21.0.
- Dropped Python 3.6 support (it was dropped in `PyArrow` 7.0).

Changes in Version 0.2.0
------------------------

- Support for PyMongo 4.0.
- Support for Python 3.10.
- Support for Windows.
- :meth:`~pymongoarrow.api.find_arrow_all` now accepts a user-provided ``projection``.
- :meth:`~pymongoarrow.api.find_arrow_all` now accepts a ``session`` object.
- Note: The supported version of ``pyarrow`` is now ``>=6,<6.1``.

Changes in Version 0.1.1
------------------------

- Fixed a bug that caused Linux wheels to be created without the appropriate
  ``manylinux`` platform tags.

Changes in Version 0.1.0
------------------------

- Support for efficiently converting find and aggregate query result sets into
  Arrow/Pandas/Numpy data structures.
- Support for patching PyMongo's APIs using :meth:`~pymongoarrow.monkey.patch_all`
- Support for loading the following `BSON types <http://bsonspec.org/spec.html>`_:

  - 64-bit binary floating point
  - 32-bit integer
  - 64-bit integer
  - Timestamp
