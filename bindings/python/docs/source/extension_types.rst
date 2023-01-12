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
            expected = Table.from_pydict(
                {
                    "_id": [i.binary for i in oids],
                    "data": [str(decs[0]), str(decs[1]), str(decs[2]), None],
                },
                ArrowSchema([("_id", binary(12)), ("data", string())]),
            )



Extension types with Pandas/NumPy
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Extension types with Pandas and NumPy are not supported, and they must use the string representation.
Like so::

        pd_schema = {"_id": np.object_, "decimal128": np.object_}
        expected = pd.DataFrame(
            data={"_id": [i.binary for i in self.oids], "decimal128": [str(i) for i in self.decimal_128s]}
        ).astype(pd_schema)
