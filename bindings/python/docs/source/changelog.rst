Changelog
=========

Changes in Version 0.3.0
------------------------
- Support for `ObjectId` `bson` type.
- Improve error message when schema contains an unsupported type.
- Add support for BSON string type.
- Add support for BSON boolean type.

Changes in Version 0.2.0
------------------------

- Support for PyMongo 4.0.
- Support for Python 3.10.
- Support for Windows.
- ``find_arrow_all`` now accepts a user-provided ``projection``.
- ``find_raw_batches`` now accepts a ``session`` object.
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

