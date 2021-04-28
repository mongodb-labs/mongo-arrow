Frequently Asked Questions
==========================

.. contents::

Why do I get ``ModuleNotFoundError: No module named 'pandas'`` when using PyMongoArrow
--------------------------------------------------------------------------------------

This error is raised when an application attempts to use a PyMongoArrow API
that returns query result sets as a :class:`pandas.DataFrame` instance without
having ``pandas`` installed in the Python environment. Since ``pandas`` is not
a direct dependency of PyMongoArrow, it is not automatically installed when
you install ``pymongoarrow`` and must be installed separately:::

  $ python -m pip install pandas
