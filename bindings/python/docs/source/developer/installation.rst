Installing from source
======================
.. highlight:: bash

System Requirements
-------------------

On macOS, you need a working modern XCode installation with the XCode
Command Line Tools. Additionally, you need CMake and pkg-config::

  $ xcode-select --install
  $ brew install cmake
  $ brew install pkg-config

On Linux, you require gcc 4.8, CMake and pkg-config.

Windows is not yet supported.

Environment Setup
-----------------

First, clone the mongo-arrow git repository::

  $ git clone https://github.com/mongodb-labs/mongo-arrow.git
  $ cd mongo-arrow/bindings/python

Additionally, create a virtualenv in which to install ``pymongoarrow``
from sources::

  $ virtualenv pymongoarrow
  $ source ./pymongoarrow/bin/activate

libbson
^^^^^^^

PyMongoArrow uses `libbson <http://mongoc.org/libbson/current/index.html>`_.
Detailed instructions for building/installing ``libbson`` can be found
`here <http://mongoc.org/libmongoc/1.17.5/installing.html#installing-the-mongodb-c-driver-libmongoc-and-bson-library-libbson>`_.

On macOS, users can install the latest ``libbson`` via Homebrew::

  $ brew install mongo-c-driver


Build
-----

In the previously created virtualenv, we first install all build dependencies
of PyMongoArrow::

  (pymongoarrow) $ pip install -r requirements/build.txt

We can now install ``pymongoarrow`` in **development mode** as follows::

  (pymongoarrow) $ python setup.py build_ext --inplace
  (pymongoarrow) $ python setup.py develop

Test
----

To run the test suite, you will need a MongoDB instance running on
``localhost`` using port ``27017``. To run the entire test suite, do::

  (pymongoarrow) $ python -m unittest discover test
