Quick Start
===========

This tutorial is intended as a comparison between using just PyMongo, versus
with **PyMongoArrow**. The reader is assumed to be familiar with basic
`PyMongo <https://pymongo.readthedocs.io/en/stable/tutorial.html>`_ and
`MongoDB <https://docs.mongodb.com>`_ concepts.


Reading Data
^^^^^^^^^^^^
The most basic way to read data using PyMongo is::

.. code-block:: python

    coll = db.benchmark
    f = list(coll.find({}, projection={"_id": 0}))
    table = pyarrow.Table.from_pylist(f)

This works, but we have to exclude the "_id" field because otherwise we get this error::

    pyarrow.lib.ArrowInvalid: Could not convert ObjectId('642f2f4720d92a85355671b3') with type ObjectId: did not recognize Python value type when inferring an Arrow data type

The workaround gets ugly (especially if you're using more than ObjectIds):

.. code-block:: pycon

    >>> f = list(coll.find({}))
    >>> for doc in f:
    ...     doc["_id"] = str(doc["_id"])
    ...
    >>> table = pyarrow.Table.from_pylist(f)
    >>> print(table)
    pyarrow.Table
    _id: string
    x: int64
    y: double

Even though this avoids the error, an unfortunate drawback is that Arrow cannot identify that it is an ObjectId,
as noted by the schema showing "_id" is a string.
The primary benefit that PyMongoArrow gives is support for BSON types through Arrow/Pandas Extension Types. This allows
you to avoid the ugly workaround:

.. code-block:: pycon

    >>> from pymongoarrow.types import ObjectIdType
    >>> schema = Schema({"_id": ObjectIdType(), "x": pyarrow.int64(), "y": pyarrow.float64()})
    >>> table = find_arrow_all(coll, {}, schema=schema)
    >>> print(table)
    pyarrow.Table
    _id: extension<arrow.py_extension_type<ObjectIdType>>
    x: int64
    y: double

And it also lets Arrow correctly identify the type! This is limited in utility for non-numeric extension types,
but if you wanted to for example, sort datetimes, it avoids unecessary casting:

.. code-block:: python

    f = list(coll.find({}, projection={"_id": 0, "x": 0}))
    naive_table = pyarrow.Table.from_pylist(f)

    schema = Schema({"time": pyarrow.timestamp("ms")})
    table = find_arrow_all(coll, {}, schema=schema)

    assert (
        table.sort_by([("time", "ascending")])["time"]
        == naive_table["time"].cast(pyarrow.timestamp("ms")).sort()
    )


Benchmarks
~~~~~~~~~~
TODO
