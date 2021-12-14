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

Alternatively, you can use the provided `build-libbson.sh` script to build it::

  $ LIBBSON_INSTALL_DIR=$(pwd)/libbson build-libbson.sh


Build
-----

In the previously created virtualenv, install PyMongoArrow and its test dependencies in editable mode::

  (pymongoarrow) $ pip install -v -e ".[test]"

If you built libbson using the `build-libbson` script then use the same `LIBBSON_INSTALL_DIR` as above:

  (pymongoarrow) $ LIBBSON_INSTALL_DIR=$(pwd)/libbson pip install -v -e ".[test]"


Test
----

To run the test suite, you will need a MongoDB instance running on
``localhost`` using port ``27017``. To run the entire test suite, do::

  (pymongoarrow) $ python -m unittest discover test
