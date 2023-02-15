.. _schema usage:

PyMongoArrow Schemas and Their Usage
====================================

While there are many similarities between `MongoDB Projections <https://www.mongodb.com/docs/manual/reference/operator/projection/positional/>`_
and :class:`pymongoarrow.api.Schema`'s, there are a few important differences:

* Schemas do not support `"dotted" notation <https://www.mongodb.com/docs/manual/core/document/#dot-notation>`_,
  instead they require that there can be only one type for any field name in a document **or any of its subdocuments**.
* Sub documents must either follow the same Schema as their parent document, or must use field names that do not conflict
  with any of the field names in the parent document.


One can also use projections to flatten the data prior to ingesting into PyMongoArrow.
The following example illustrates how to do it with a very simple nested document structure.

.. code-block:: python

   >>> from pymongo import MongoClient
   >>> from pymongoarrow.api import Schema, find_pandas_all
   >>> from pymongoarrow.types import (
   >>>     ObjectIdType,
   >>> )
   >>> coll = MongoClient(username="user", password="password").db.coll
   >>> coll.insert_many([{
   >>>     "prop": {
   >>>         "Name": "foo",
   >>>         "Start": 0,
   >>>     }
   >>> }, {
   >>>     "prop": {
   >>>         "Name": "bar",
   >>>         "Start": 10,
   >>>     }
   >>> },])
   >>> df=find_pandas_all(coll,
   >>> {"prop.Start": {'$gte':0,'$lte':10,}},
   >>> projection={
   >>>     "propName": "$prop.Name",
   >>>     "propStart": "$prop.Start"
   >>> },
   >>> schema=Schema({"_id": ObjectIdType(), "propStart": int, "propName": str}))
   >>> print(df)
                                    _id  propStart propName
   0  b'c\xec2\x98R(\xc9\x1e@#\xcc\xbb'          0      foo
   1  b'c\xec2\x98R(\xc9\x1e@#\xcc\xbc'         10      bar


The same thing can also be accomplished using aggregation:

.. code-block:: python

   >>> from pymongo import MongoClient
   >>> from pymongoarrow.api import Schema, find_pandas_all, aggregate_pandas_all
   ...
   >>> df=aggregate_pandas_all(coll, pipeline=[
   >>> {
   >>>   "$match": {
   >>>     "prop.Start": {
   >>>       "$gte": 0,
   >>>       "$lte": 10
   >>>     }
   >>>   }
   >>> },
   >>> {
   >>>   "$project": {
   >>>     "propStart": "$prop.Start",
   >>>     "propName": "$prop.Name",
   >>>
   >>>   }
   >>> }
   >>> ])
