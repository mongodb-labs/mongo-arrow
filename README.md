# mongo-arrow
Tools for using Apache Arrow with MongoDB

## Apache Arrow
We utilize Apache Arrow to offer fast and easy conversion of MongoDB query result sets to multiple numerical data formats popular among developers including NumPy ndarrays, Pandas DataFrames, parquet files, csv, and more.

We chose Arrow for this because of its unique set of characteristics:
- language-independent
- columnar memory format for flat and hierarchical data,
- organized for efficient analytic operations on modern hardware like CPUs and GPUs
- zero-copy reads for lightning-fast data access without serialization overhead
  - it was simple and fast, and from our perspective Apache Arrow is ideal for processing and transport of large datasets in high-performance applications.

As reference points for our implementation, we also took a look at BigQuery’s Pandas integration, pandas methods to handle JSON/semi-structured data, the Snowflake Python connector, and Dask.DataFrame.


## How it Works
Our implementation relies upon a user-specified data schema to marshall query result sets into tabular form.
Example
```
from pymongoarrow.api import Schema
schema = Schema({'_id': int, 'amount': float, 'last_updated': datetime})
```

You can install PyMongoArrow on your local machine using Pip:
`$ python -m pip install pymongoarrow`

You can export data from MongoDB to a pandas dataframe easily using something like:
```
df = production.invoices.find_pandas_all({'amount': {'$gt': 100.00}}, schema=invoices)
```

Since PyMongoArrow can automatically infer the schema from the first batch of data, this can be
further simplifed to:

```
df = production.invoices.find_pandas_all({'amount': {'$gt': 100.00}})
```

## Final Thoughts
This library is in the early stages of development, and so it's possible the API may change in the future -
we definitely want to continue expanding it. We welcome your feedback as we continue to explore and build this tool.
