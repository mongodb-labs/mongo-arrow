Frequently Asked Questions
==========================

.. contents::

Why do I get ``ModuleNotFoundError: No module named 'polars'`` when using PyMongoArrow?
---------------------------------------------------------------------------------------

This error is raised when an application attempts to use a PyMongoArrow API
that returns query result sets as a :class:`polars.DataFrame` instance without
having ``polars`` installed in the Python environment. Since ``polars`` is not
a direct dependency of PyMongoArrow, it is not automatically installed when
you install ``pymongoarrow`` and must be installed separately::

  $ python -m pip install polars
