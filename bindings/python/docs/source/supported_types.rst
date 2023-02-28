.. _type support:

Supported Types
===============

PyMongoArrow currently supports a small subset of all BSON types.
Support for additional types will be added in subsequent releases.

.. note:: PyMongoArrow does not currently fully support extension types with Pandas/NumPy or Arrow.
   However, they can be used in schemas.
   This means that ObjectId and Decimal128 are not fully supported in Pandas DataFrames or Arrow Tables.
   Instead, the schema type will be converted to a string or object representation of the type.
   For more information see :doc:`extension_types`.

.. note:: For more information about BSON types, see the
   `BSON specification <http://bsonspec.org/spec.html>`_.

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
     - :class:`py.bytes`, :class:`bson.ObjectId`, an instance of :class:`pymongoarrow.types.ObjectIdType`, an instance of :class:`pyarrow.FixedSizeBinaryScalar`
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

Null Values and Conversion to Pandas DataFrames
-----------------------------------------------

In Arrow, all Arrays are always nullable. 
Pandas has experimental nullable data types as, e.g., "Int64" (note the capital "I").
You can instruct Arrow to create a pandas DataFrame using nullable dtypes 
with the code below (taken from `here <https://arrow.apache.org/docs/python/pandas.html>`_)

.. code-block:: pycon

   >>> dtype_mapping = {
   ...     pa.int8(): pd.Int8Dtype(),
   ...     pa.int16(): pd.Int16Dtype(),
   ...     pa.int32(): pd.Int32Dtype(),
   ...     pa.int64(): pd.Int64Dtype(),
   ...     pa.uint8(): pd.UInt8Dtype(),
   ...     pa.uint16(): pd.UInt16Dtype(),
   ...     pa.uint32(): pd.UInt32Dtype(),
   ...     pa.uint64(): pd.UInt64Dtype(),
   ...     pa.bool_(): pd.BooleanDtype(),
   ...     pa.float32(): pd.Float32Dtype(),
   ...     pa.float64(): pd.Float64Dtype(),
   ...     pa.string(): pd.StringDtype(),
   ... }
   ... 
   ... df = arrow_table.to_pandas(types_mapper=dtype_mapping.get, split_blocks=True, self_destruct=True)
   ... del arrow_table

Defining a conversion for `pa.string()` in addition converts Arrow strings to NumPy strings, and not objects.
