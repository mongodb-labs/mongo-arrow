Extension Types
===============

This tutorial is intended as an introduction to working with
**PyMongoArrow** and its corresponding extension types. The reader is assumed to be familiar with basic
`PyMongo <https://pymongo.readthedocs.io/en/stable/tutorial.html>`_ and
`MongoDB <https://docs.mongodb.com>`_ concepts.

Extension types with Arrow
^^^^^^^^^^^^^^^^^^^^^^^^^^
Both extension types, :class:`pymongoarrow.types.ObjectIdType` and :class:`pymongoarrow.types.Decimal128StringType`, are only partially supported in PyArrow. They will work when used in a
schema, but will show up in the table as a `fixed_size_binary(12)` or `string` respectively::

        schema = Schema({"_id": ObjectIdType(), "data": Decimal128StringType()})
        table = find_arrow_all(coll, {}, schema=schema)
        print(table)
        >>> pyarrow.Table
        >>> _id: fixed_size_binary[12]
        >>> data: string
        >>> ----
        >>> _id: [[63C003BF0A1D5281D33B0AFD,63C003BF0A1D5281D33B0AFE,63C003BF0A1D5281D33B0AFF,63C003BF0A1D5281D33B0B00]]
        >>> data: [["0.1","1.0","0.00001",null]]
        >>> ...



Extension types with Pandas/NumPy
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Extension types with Pandas/NumPy are only partially supported. They will work when used in a
schema, but will show up in the table as a :py:`pandas.object`::

        schema = Schema({"_id": ObjectIdType(), "data": Decimal128StringType()})
        table = find_pandas_all(coll, {}, schema=schema)
        print(table.info())
        >>> RangeIndex: 4 entries, 0 to 3
        >>> Data columns (total 2 columns):
        >>>  #   Column      Non-Null Count  Dtype
        >>> ---  ------      --------------  -----
        >>>  0   _id         4 non-null      object
        >>>  1   data        3 non-null      object
