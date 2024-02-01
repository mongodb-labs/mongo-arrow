Running Benchmarks
==================

.. highlight:: bash

System Requirements
-------------------

To run the benchmarks, you need the `asv <https://pypi.org/project/asv/>`_ package,
which can then be invoked like so::

  $ asv run --strict --python=`which python`

or you can run with tox as:

  $ tox -e benchmarks
