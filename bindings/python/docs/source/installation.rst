Installing / Upgrading
======================
.. highlight:: bash

System Compatibility
--------------------

PyMongoArrow is regularly built and tested on macOS and Linux
(Ubuntu 20.04).

Python Compatibility
--------------------

PyMongoArrow is currently compatible with CPython 3.7, 3.8 and 3.9.

Using Pip
---------
**PyMongoArrow** is available on
`PyPI <http://pypi.python.org/pypi/pymongo/>`_. We recommend using
`pip <http://pypi.python.org/pypi/pip>`_ to install ``pymongoarrow``
on all platforms::

  $ python -m pip install pymongoarrow

To get a specific version of pymongo::

  $ python -m pip install pymongoarrow==0.1.1

To upgrade using pip::

  $ python -m pip install --upgrade pymongoarrow

.. attention:: If the install fails due to an error like ``ValueError: Could
  not find "libbson-1.0" library`` it means that ``pip`` failed to find a
  suitable wheel for your platform.  We recommend first ensuring you have
  ``pip`` >= 20.3 installed. To upgrade ``pip`` run::

     $ python -m pip install --upgrade pip

   You can then attempt to re-install ``pymongoarrow``.

   We currently distribute wheels for macOS, Windows, and Linux on x86_64
   architectures.

**PyMongoArrow** is also available for ``conda`` users by running::

  $ conda install --channel conda-forge pymongoarrow


If the above options still do not allow you to install ``pymongoarrow`` on your
system, you will have to install from source, see :doc:`developer/installation`.


Dependencies
^^^^^^^^^^^^

PyMongoArrow requires:

- PyMongo>=3.11 (PyMongo 4.0 is supported from 0.2)
- PyArrow>=7,<7.1

To use PyMongoArrow with a PyMongo feature that requires an optional
dependency, users must install PyMongo with the given dependency manually.

.. note:: PyMongo's optional dependencies are detailed
   `here <https://pymongo.readthedocs.io/en/stable/installation.html#dependencies>`_.

For example, to use PyMongoArrow with MongoDB Atlas' ``mongodb+srv://`` URIs
users must install PyMongo with the ``srv`` extra in addition to installing
PyMongoArrow::

  $ python -m pip install 'pymongo[srv]' pymongoarrow

Applications intending to use PyMongoArrow APIs that return query result sets
as :class:`pandas.DataFrame` instances (e.g. :meth:`~pymongoarrow.api.find_pandas_all`)
must also have ``pandas`` installed::

  $ python -m pip install pandas

Installing from source
----------------------

See :doc:`developer/installation`.
