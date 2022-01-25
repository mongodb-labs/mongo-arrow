.. _type support:

Supported Types
===============

PyMongoArrow currently supports a small subset of all BSON types.
Support for additional types will be added in subsequent releases.

.. note:: For more information about BSON types, see the
   `BSON specification <http://bsonspec.org/spec.html>`_.

.. list-table::
   :widths: auto
   :header-rows: 1

   * - BSON Type
     - Type Identifiers
   * - ObjectId
     - :class:`py.bytes`, :class:`bson.ObjectId`, an instance of :class:`pyarrow.FixedSizeBinaryScalar`
   * - 64-bit binary floating point
     - :class:`py.float`, an instance of :meth:`pyarrow.float64`
   * - 32-bit integer
     - an instance of :meth:`pyarrow.int32`
   * - 64-bit integer
     - :class:`~py.int`, :class:`bson.int64.Int64`, an instance of :meth:`pyarrow.int64`
   * - UTC datetime
     - an instance of :class:`~pyarrow.timestamp` with ``ms`` resolution, :class:`py.datetime.datetime`

Type identifiers can be used to specify that a field is of a certain type
during :class:`pymongoarrow.api.Schema` declaration. For example, if your data
has fields 'f1' and 'f2' bearing types 32-bit integer and UTC datetime
respectively, and '_id' that is an `ObjectId`, your schema can be defined as::

  schema = Schema({
    '_id': ObjectId,
    'f1': pyarrow.int32(),
    'f2': pyarrow.timestamp('ms')
})
