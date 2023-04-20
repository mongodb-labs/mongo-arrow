.. _type support:

Data Types
==========

PyMongoArrow supports a majority of the BSON types.
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
     - An instance of :class:`pyarrow.list_`,
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
   * - Binary data
     - :class:`bson.Binary`, an instance of :class:`pymongoarrow.types.BinaryType`, an instance of :class:`pymongoarrow.pandas_types.PandasBinary`.
   * - JavaScript code
     - :class:`bson.Code`, an instance of :class:`pymongoarrow.types.CodeType`, an instance of :class:`pymongoarrow.pandas_types.PandasCode`.

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


Embedded Array Considerations
-----------------------------

The schema used for an Embedded Array must use the `pyarrow.list_()` type,
so that the type of the array elements can be specified.  For example,

.. code-block: python

  from pyarrow import list_, float64
  schema = Schema({'_id': ObjectId,
    'location': {'coordinates': list_(float64())}
  })


Extension Types
---------------

The ``ObjectId``, ``Decimal128``, ``Binary data`` and ``JavaScript code``
are implemented as extension types for PyArrow and Pandas.
For arrow tables, values of these types will have the appropriate
``pymongoarrow`` extension type (e.g. :class:`pymongoarrow.types.ObjectIdType`).  The appropriate ``bson`` Python object can be obtained using the ``.as_py()`` method,
or by calling ``.to_pylist()`` on the table.

.. code-block:: pycon

  >>> from pymongo import MongoClient
  >>> from bson import ObjectId
  >>> from pymongoarrow.api import find_arrow_all
  >>> client = MongoClient()
  >>> coll = client.test.test
  >>> coll.insert_many([{"_id": ObjectId(), "foo": 100}, {"_id": ObjectId(), "foo": 200}])
  <pymongo.results.InsertManyResult at 0x1080a72b0>
  >>> table = find_arrow_all(coll, {})
  >>> table
  pyarrow.Table
  _id: extension<arrow.py_extension_type<ObjectIdType>>
  foo: int32
  ----
  _id: [[64408B0D5AC9E208AF220142,64408B0D5AC9E208AF220143]]
  foo: [[100,200]]
  >>> table["_id"][0]
  <pyarrow.ObjectIdScalar: ObjectId('64408b0d5ac9e208af220142')>
  >>> table["_id"][0].as_py()
  ObjectId('64408b0d5ac9e208af220142')
  >>> table.to_pylist()
  [{'_id': ObjectId('64408b0d5ac9e208af220142'), 'foo': 100},
   {'_id': ObjectId('64408b0d5ac9e208af220143'), 'foo': 200}]

When converting to pandas, the extension type columns will have an appropriate
``pymongoarrow`` extension type (e.g. :class:`pymongoarrow.pandas_types.PandasDecimal128`).  The value of the element in the
dataframe will be the appropriate ``bson`` type.

.. code-block:: pycon

  >>> from pymongo import MongoClient
  >>> from bson import Decimal128
  >>> from pymongoarrow.api import find_pandas_all
  >>> client = MongoClient()
  >>> coll = client.test.test
  >>> coll.insert_many([{"foo": Decimal128("0.1")}, {"foo": Decimal128("0.1")}])
  <pymongo.results.InsertManyResult at 0x1080a72b0>
  >>> df = find_pandas_all(coll, {})
  >>> df
                          _id  foo
  0  64408bf65ac9e208af220144  0.1
  1  64408bf65ac9e208af220145  0.1
  >>> df["foo"].dtype
  <pymongoarrow.pandas_types.PandasDecimal128 at 0x11fe0ae90>
  >>> df["foo"][0]
  Decimal128('0.1')
  >>> df["_id"][0]
  ObjectId('64408bf65ac9e208af220144')


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
   ... df = arrow_table.to_pandas(
   ...     types_mapper=dtype_mapping.get, split_blocks=True, self_destruct=True
   ... )
   ... del arrow_table

Defining a conversion for `pa.string()` in addition converts Arrow strings to NumPy strings, and not objects.
