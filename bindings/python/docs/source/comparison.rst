Quick Start
===========

This tutorial is intended as a comparison between using just PyMongo, versus
with **PyMongoArrow**. The reader is assumed to be familiar with basic
`PyMongo <https://pymongo.readthedocs.io/en/stable/tutorial.html>`_ and
`MongoDB <https://docs.mongodb.com>`_ concepts.


Reading Data
^^^^^^^^^^^^
The most basic way to read data using PyMongo is:

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
The primary benefit that PyMongoArrow gives is support for BSON types through Arrow/Pandas Extension Types. This allows you to avoid the ugly workaround:

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
but if you wanted to for example, sort datetimes, it avoids unnecessary casting:

.. code-block:: python

    f = list(coll.find({}, projection={"_id": 0, "x": 0}))
    naive_table = pyarrow.Table.from_pylist(f)

    schema = Schema({"time": pyarrow.timestamp("ms")})
    table = find_arrow_all(coll, {}, schema=schema)

    assert (
        table.sort_by([("time", "ascending")])["time"]
        == naive_table["time"].cast(pyarrow.timestamp("ms")).sort()
    )

Additionally, PyMongoArrow supports Pandas extension types.
With PyMongo, a Decimal128 value behaves as follows:

.. code-block:: python

    coll = client.test.test
    coll.insert_many([{"value": Decimal128(str(i))} for i in range(200)])
    cursor = coll.find({})
    df = pd.DataFrame(list(cursor))
    print(df.dtypes)
    # _id      object
    # value    object

The equivalent in PyMongoArrow would be:

.. code-block:: python

    from pymongoarrow.api import find_pandas_all

    coll = client.test.test
    coll.insert_many([{"value": Decimal128(str(i))} for i in range(200)])
    df = find_pandas_all(coll, {})
    print(df.dtypes)
    # _id      bson_PandasObjectId
    # value    bson_PandasDecimal128

In both cases the underlying values are the bson class type:

.. code-block:: python

    print(df["value"][0])
    Decimal128("0")


Writing Data
~~~~~~~~~~~~

Writing data from an Arrow table using PyMongo looks like the following:

.. code-block:: python

    data = arrow_table.to_pylist()
    db.collname.insert_many(data)

The equivalent in PyMongoArrow is:

.. code-block:: python

    from pymongoarrow.api import write

    write(db.collname, arrow_table)

As of PyMongoArrow 1.0, the main advantage to using the ``write`` function
is that it will iterate over the arrow table/ data frame / numpy array
and not convert the entire object to a list.


Benchmarks
~~~~~~~~~~

The following measurements were taken with PyMongoArrow 1.0 and PyMongo 4.4.
For insertions, the library performs about the same as when using PyMongo
(conventional), and uses the same amount of memory.::

    ProfileInsertSmall.peakmem_insert_conventional      107M
    ProfileInsertSmall.peakmem_insert_arrow             108M
    ProfileInsertSmall.time_insert_conventional         202±0.8ms
    ProfileInsertSmall.time_insert_arrow                181±0.4ms

    ProfileInsertLarge.peakmem_insert_arrow             127M
    ProfileInsertLarge.peakmem_insert_conventional      125M
    ProfileInsertLarge.time_insert_arrow                425±1ms
    ProfileInsertLarge.time_insert_conventional         440±1ms

For reads, the library is somewhat slower for small documents and nested
documents, but faster for large documents .  It uses less memory in all cases::

    ProfileReadSmall.peakmem_conventional_arrow     85.8M
    ProfileReadSmall.peakmem_to_arrow               83.1M
    ProfileReadSmall.time_conventional_arrow        38.1±0.3ms
    ProfileReadSmall.time_to_arrow                  60.8±0.3ms

    ProfileReadLarge.peakmem_conventional_arrow     138M
    ProfileReadLarge.peakmem_to_arrow               106M
    ProfileReadLarge.time_conventional_ndarray      243±20ms
    ProfileReadLarge.time_to_arrow                  186±0.8ms

    ProfileReadDocument.peakmem_conventional_arrow  209M
    ProfileReadDocument.peakmem_to_arrow            152M
    ProfileReadDocument.time_conventional_arrow     865±7ms
    ProfileReadDocument.time_to_arrow               937±1ms
