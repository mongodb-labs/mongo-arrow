# PyMongoArrow

[![PyPI Version](https://img.shields.io/pypi/v/pymongoarrow)](https://pypi.org/project/pymongoarrow)
[![Python Versions](https://img.shields.io/pypi/pyversions/pymongoarrow)](https://pypi.org/project/pymongoarrow)
[![Monthly Downloads](https://static.pepy.tech/badge/pymongoarrow/month)](https://pepy.tech/project/pymongoarrow)
[![Documentation Status](https://readthedocs.org/projects/mongo-arrow/badge/?version=stable)](http://mongo-arrow.readthedocs.io/en/stable/?badge=stable)

**PyMongoArrow** is a companion library to PyMongo that contains tools
for loading MongoDB query result sets as Apache Arrow tables, Pandas
DataFrames or NumPy arrays.

```pycon
>>> from pymongoarrow.monkey import patch_all
... patch_all()
... from pymongoarrow.api import Schema
... schema = Schema({"_id": int, "qty": float})
... from pymongo import MongoClient
... client = MongoClient()
... client.db.data.insert_many(
...     [{"_id": 1, "qty": 25.4}, {"_id": 2, "qty": 16.9}, {"_id": 3, "qty": 2.3}]
... )
... data_frame = client.db.test.find_pandas_all({}, schema=schema)
... data_frame
   _id   qty
0    1  25.4
1    2  16.9
2    3   2.3
... arrow_table = client.db.test.find_arrow_all({}, schema=schema)
# The schema may also be omitted
... arrow_table = client.db.test.find_arrow_all({})
... arrow_table
pyarrow.Table
_id: int64
qty: double
... ndarrays = client.db.test.find_numpy_all({}, schema=schema)
... ndarrays
{'_id': array([1, 2, 3]), 'qty': array([25.4, 16.9,  2.3])}
```

**PyMongoArrow** is the recommended way to materialize MongoDB query
result sets as contiguous-in-memory, typed arrays suited for in-memory
analytical processing applications.

## Installing PyMongoArrow

PyMongoArrow is available on PyPI:

```bash
python -m pip install pymongoarrow
```

To use PyMongoArrow with MongoDB Atlas' `mongodb+srv://` URIs, you will
need to also install PyMongo with the `srv` extra:

```bash
python -m pip install 'pymongo[srv]' pymongoarrow
```

To use PyMongoArrow APIs that return query result sets as pandas
DataFrame instances, you will also need to have the `pandas` package
installed:

```bash
python -m pip install pandas
```

Note: `pymongoarrow` is not supported or tested on big-endian systems
(e.g. Linux s390x).

## Development Install

See the instructions on [Read the
Docs](https://mongo-arrow.readthedocs.io/en/latest).

## Documentation

Full documentation is available on [Read the
Docs](https://mongo-arrow.readthedocs.io/en/latest).
