============
PyMongoArrow
============
:Info: A companion library to PyMongo that makes it easy to move data
       between MongoDB and Apache Arrow. See
       `GitHub <https://github.com/mongodb-labs/mongo-arrow/tree/main/bindings/python>`_
       for the latest source.
:Documentation: Available at `mongo-arrow.readthedocs.io <https://mongo-arrow.readthedocs.io/en/latest/>`_.
:Author: Prashant Mital

**PyMongoArrow** is a companion library to PyMongo that contains tools
for loading MongoDB query result sets as Apache Arrow tables, Pandas
DataFrames or NumPy arrays.

.. code-block:: python

  >>> from pymongoarrow.monkey import patch_all
  >>> patch_all()
  >>> from pymongoarrow.api import Schema
  >>> schema = Schema({'_id': int, 'qty': float})
  >>> from pymongo import MongoClient
  >>> client = MongoClient()
  >>> client.db.data.insert_many([{'_id': 1, 'qty': 25.4}, {'_id': 2, 'qty': 16.9}, {'_id': 3, 'qty': 2.3}])
  >>> data_frame = client.db.test.find_pandas_all({}, schema=schema)
  >>> data_frame
     _id   qty
  0    1  25.4
  1    2  16.9
  2    3   2.3
  >>> arrow_table = client.db.test.find_arrow_all({}, schema=schema)
  >>> arrow_table
  pyarrow.Table
  _id: int64
  qty: double
  >>> ndarrays = client.db.test.find_numpy_all({}, schema=schema)
  >>> ndarrays
  {'_id': array([1, 2, 3]), 'qty': array([25.4, 16.9,  2.3])}

**PyMongoArrow** is the recommended way to
materialize MongoDB query result sets as contiguous-in-memory, typed arrays
suited for in-memory analytical processing applications.

Installing PyMongoArrow
=======================
PyMongoArrow is available on PyPI:::

  $ python -m pip install pymongoarrow

PyMongoArrow wheels are available for macOS and Linux on x86_64 architectures.

Documentation
=============
Full documentation is available on `Read the Docs <https://mongo-arrow.readthedocs.io/en/latest/>`_.
