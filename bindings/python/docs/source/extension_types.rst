Extension Types
===============

This tutorial is intended as an introduction to working with
**PyMongoArrow** and it's corresponding extension types. The reader is assumed to be familiar with basic
`PyMongo <https://pymongo.readthedocs.io/en/stable/tutorial.html>`_ and
`MongoDB <https://docs.mongodb.com>`_ concepts.

Extension types with Arrow
^^^^^^^^^^^^^^^^^^^^^^^^^^
Both extension types, ObjectId and Decimal128, are supported in Arrow. This means that when using
a PyArrow table, you can use the corresponding PyMongoArrow extension type like so::

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
Extension types with Pandas and NumPy are not supported, and they both use the object representation.
Like so::

        schema = Schema({"_id": ObjectIdType(), "data": Decimal128StringType()})
        table = find_pandas_all(coll, {}, schema=schema)
        print(table.info())
        >>> RangeIndex: 4 entries, 0 to 3
        >>> Data columns (total 2 columns):
        >>>  #   Column      Non-Null Count  Dtype
        >>> ---  ------      --------------  -----
        >>>  0   _id         4 non-null      object
        >>>  1   data        3 non-null      object
