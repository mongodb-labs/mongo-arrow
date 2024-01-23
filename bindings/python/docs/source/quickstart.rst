Quick Start
===========

This tutorial is intended as an introduction to working with
**PyMongoArrow**. The reader is assumed to be familiar with basic
`PyMongo <https://pymongo.readthedocs.io/en/stable/tutorial.html>`_ and
`MongoDB <https://docs.mongodb.com>`_ concepts.

Prerequisites
-------------
Before we start, make sure that you have the **PyMongoArrow** distribution
:doc:`installed <installation>`. In the Python shell, the following should
run without raising an exception::

  import pymongoarrow as pma

This tutorial also assumes that a MongoDB instance is running on the
default host and port. Assuming you have `downloaded and installed
<https://docs.mongodb.com/manual/installation/>`_ MongoDB, you can start
it like so:

.. code-block:: bash

  $ mongod

Extending PyMongo
^^^^^^^^^^^^^^^^^
The :mod:`pymongoarrow.monkey` module provides an interface to patch PyMongo,
in place, and add **PyMongoArrow**'s functionality directly to
:class:`~pymongo.collection.Collection` instances::

  from pymongoarrow.monkey import patch_all
  patch_all()

After running :meth:`~pymongoarrow.monkey.patch_all`, new instances of
:class:`~pymongo.collection.Collection` will have PyMongoArrow's APIs,
e.g. :meth:`~pymongoarrow.api.find_pandas_all`.

.. note:: Users can also directly use any of **PyMongoArrow**'s APIs
   by importing them from :mod:`pymongoarrow.api`. The only difference in
   usage would be the need to manually pass the instance of
   :class:`~pymongo.collection.Collection` on which the operation is to be
   run as the first argument when directly using the API method.

Test data
^^^^^^^^^
Before we begin, we must first add some data to our cluster that we can
query. We can do so using **PyMongo**::

  from datetime import datetime
  from pymongo import MongoClient
  client = MongoClient()
  client.db.data.insert_many([
      {'_id': 1, 'amount': 21, 'last_updated': datetime(2020, 12, 10, 1, 3, 1), 'account': { 'name': "Customer1", 'account_number': 1}}, "txns": [1, 2, 3]},
      {'_id': 2, 'amount': 16, 'last_updated': datetime(2020, 7, 23, 6, 7, 11), 'account': { 'name': "Customer2", 'account_number': 2}}, "txns": [1, 2, 3]},
      {'_id': 3, 'amount': 3, 'last_updated': datetime(2021, 3, 10, 18, 43, 9), 'account': { 'name': "Customer3", 'account_number': 3}}, "txns": [1, 2, 3]},
      {'_id': 4, 'amount': 0, 'last_updated': datetime(2021, 2, 25, 3, 50, 31), 'account': { 'name': "Customer4", 'account_number': 4}}, "txns": [1, 2, 3]}])

Defining the schema
-------------------
**PyMongoArrow** relies upon a data schema to marshall
query result sets into tabular form. This schema can either be automatically inferred from the data,
or provided by the user. Users can define the schema by
instantiating :class:`pymongoarrow.api.Schema` using a mapping of field names
to type-specifiers, e.g.::

  from pymongoarrow.api import Schema
  schema = Schema({'_id': int, 'amount': float, 'last_updated': datetime})

There are multiple permissible type-identifiers for each supported BSON type.
For a full-list of data types and associated type-identifiers see
:doc:`data_types`.

Nested data (embedded documents) are also supported::

  from pymongoarrow.api import Schema
  schema = Schema({'_id': int, 'amount': float, 'account': { 'name': str, 'account_number': int}})

Arrays (and nested arrays) are also supported::

  from pymongoarrow.api import Schema
  schema = Schema({'_id': int, 'amount': float, 'txns': list_(int32())})

.. note::

   For all of the examples below, the schema can be omitted like so::

    arrow_table = client.db.data.find_arrow_all({'amount': {'$gt': 0}})

   In this case, PyMongoArrow will try to automatically apply a schema based on
   the data contained in the first batch.


Find operations
---------------
We are now ready to query our data. Let's start by running a ``find``
operation to load all records with a non-zero ``amount`` as a
:class:`pandas.DataFrame`::

  df = client.db.data.find_pandas_all({'amount': {'$gt': 0}}, schema=schema)

We can also load the same result set as a :class:`pyarrow.Table` instance::

  arrow_table = client.db.data.find_arrow_all({'amount': {'$gt': 0}}, schema=schema)

Or as :class:`numpy.ndarray` instances::

  ndarrays = client.db.data.find_numpy_all({'amount': {'$gt': 0}}, schema=schema)

In the NumPy case, the return value is a dictionary where the keys are field
names and values are the corresponding arrays.

Nested data (embedded documents) are also supported::

  from pymongoarrow.api import Schema
  schema = Schema({'_id': int, 'amount': float, 'account': { 'name': str, 'account_number': int}})
  arrow_table = client.db.data.find_arrow_all({'amount': {'$gt': 0}}, schema=schema)

Arrays (and nested arrays) are also supported::

  from pymongoarrow.api import Schema
  from pyarrow import int32, list_
  schema = Schema({'_id': int, 'amount': float, 'txns': list_(int32())})
  arrow_table = client.db.data.find_arrow_all({'amount': {'$gt': 0}}, schema=schema)

Aggregate operations
--------------------
Running ``aggregate`` operations is similar to ``find``. Here is an example of
an aggregation that loads all records with an ``amount`` less than 10::

  # pandas
  df = client.db.data.aggregate_pandas_all([{'$match': {'amount': {'$lte': 10}}}], schema=schema)
  # arrow
  arrow_table = client.db.data.aggregate_arrow_all([{'$match': {'amount': {'$lte': 10}}}], schema=schema)
  # numpy
  ndarrays = client.db.data.aggregate_numpy_all([{'$match': {'amount': {'$lte': 10}}}], schema=schema)

Nested data (embedded documents) are also supported::

  from pymongoarrow.api import Schema
  schema = Schema({'_id': int, 'amount': float, 'account': { 'name': str, 'account_number': int}})
  arrow_table = client.db.data.find_arrow_all({'amount': {'$gt': 0}}, schema=schema)
  arrow_table = client.db.data.aggregate_arrow_all([{'$match': {'amount': {'$lte': 10}}}], schema=schema)



Writing to MongoDB
-----------------------
Result sets that have been loaded as Arrow's :class:`~pyarrow.Table` type, Pandas'
:class:`~pandas.DataFrame` type, or NumPy's :class:`~numpy.ndarray` type can
be easily written to your MongoDB database using the :meth:`~pymongoarrow.api.write` function::

  from pymongoarrow.api import write
  from pymongo import MongoClient
  coll = MongoClient().db.my_collection
  write(coll, df)
  write(coll, arrow_table)
  write(coll, ndarrays)

Writing to other formats
------------------------
Once result sets have been loaded, one can then write them to any format that the package supports.

For example, to write the table referenced by the variable ``arrow_table`` to a Parquet
file ``example.parquet``, run::

  import pyarrow.parquet as pq
  pq.write_table(arrow_table, 'example.parquet')

Pandas also supports writing :class:`~pandas.DataFrame` instances to a variety
of formats including CSV, and HDF. To write the data frame
referenced by the variable ``df`` to a CSV file ``out.csv``, for example, run::

  df.to_csv('out.csv', index=False)

.. note::

  Nested data is supported for parquet read/write but is not well supported
  by Arrow or Pandas for CSV read/write.
