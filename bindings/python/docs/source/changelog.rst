Changelog
=========

Changes in Version 0.1.0
------------------------

- Support for efficiently converting find and aggregate query result sets into
  Arrow/Pandas/Numpy datastructures.
- Support for patching PyMongo's APIs using :meth:`~pymongoarrow.monkey.patch_all`
- Support for loading the following `BSON types <http://bsonspec.org/spec.html>`_:

  - 64-bit binary floating point
  - 32-bit integer
  - 64-bit integer
  - Timestamp

