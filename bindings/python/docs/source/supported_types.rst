.. _type support:

Supported Types
===============

PyMongoArrow currently supports a small subset of all BSON types.
Support for additional types will be added in subsequent releases.


.. note:: For more information about BSON types, see the
   `BSON specification <http://bsonspec.org/spec.html>`_.

.. note:: ``Decimal128`` types are only supported on little-endian systems.
   On big-endian systems, ``null`` will be used.

.. list-table::
   :widths: auto
   :header-rows: 1

   * - BSON Type
     - Type Identifiers
   * - String
     - :class:`py.str`, an instance of :class:`pyarrow.string`
   * - Embedded document
     - :class:`py.dict`, and instance of :class:`pyarrow.struct`
   * - Embedded array
     - :class:`py.list`, an instance of :class:`pyarrow.list_`,
   * - ObjectId
     - :class:`py.bytes`, :class:`bson.ObjectId`, an instance of :class:`pymongoarrow.types.ObjectIdType`, an instance of :class:`pymongoarrow.pandas_types.PandasObjectId`
   * - Decimal128
     - :class:`bson.Decimal128`, an instance of :class:`pymongoarrow.types.Decimal128Type`, an instance of :class:`pymongoarrow.pandas_types.PandasDecimal128`.
   * - Boolean
     - an instance of :class:`~pyarrow.bool_`, :class:`~py.bool`
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

Unsupported data types in a schema cause a ``ValueError`` identifying the
field and its data type.
