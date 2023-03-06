.. _schema usage:

Schema Examples
===============

The following are a few examples of usage of PyMongoArrow Schemas in common situations.


Nested Data With Schema
-----------------------

With aggregate or find methods, you can provide a schema for nested data using the struct object. Note that there can be conflicting
names in sub-documents compared to their parent documents.

.. code-block:: pycon

   >>> from pymongo import MongoClient
   ... from pymongoarrow.api import Schema, find_arrow_all
   ... from pyarrow import struct, field, int32
   ... coll = MongoClient().db.coll
   ... coll.insert_many(
   ...     [
   ...         {"start": "string", "prop": {"name": "foo", "start": 0}},
   ...         {"start": "string", "prop": {"name": "bar", "start": 10}},
   ...     ]
   ... )
   ... arrow_table = find_arrow_all(
   ...     coll, {}, schema=Schema({"start": str, "prop": struct([field("start", int32())])})
   ... )
   ... print(arrow_table)
   pyarrow.Table
   start: string
   prop: struct<start: int32>
     child 0, start: int32
   ----
   start: [["string","string"]]
   prop: [
     -- is_valid: all not null
     -- child 0 type: int32
   [0,10]]

For Pandas and NumPy you can do the same exact thing:

.. code-block:: pycon

   >>> df = find_pandas_all(
   ...     coll, {}, schema=Schema({"start": str, "prop": struct([field("start", int32())])})
   ... )
   ... print(df)
       start           prop
   0  string   {'start': 0}
   1  string  {'start': 10}


Nested Data With Projections
----------------------------

One can also use projections to flatten the data prior to ingesting into PyMongoArrow.
The following example illustrates how to do it with a very simple nested document structure.

.. code-block:: pycon

   >>> df = find_pandas_all(
   ...     coll,
   ...     {
   ...         "prop.start": {
   ...             "$gte": 0,
   ...             "$lte": 10,
   ...         }
   ...     },
   ...     projection={"propName": "$prop.name", "propStart": "$prop.start"},
   ...     schema=Schema({"_id": ObjectIdType(), "propStart": int, "propName": str}),
   ... )
   ... print(df)
                                    _id  propStart propName
   0  b'c\xec2\x98R(\xc9\x1e@#\xcc\xbb'          0      foo
   1  b'c\xec2\x98R(\xc9\x1e@#\xcc\xbc'         10      bar


For aggregate you can flatten the fields using the `$project` stage, like so:

.. code-block:: pycon

   >>> df = aggregate_pandas_all(
   ...     coll,
   ...     pipeline=[
   ...         {"$match": {"prop.start": {"$gte": 0, "$lte": 10}}},
   ...         {
   ...             "$project": {
   ...                 "propStart": "$prop.start",
   ...                 "propName": "$prop.name",
   ...             }
   ...         },
   ...     ],
   ... )
