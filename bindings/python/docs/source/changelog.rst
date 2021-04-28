Changelog
=========

Changes in Version 0.2.0
------------------------


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

