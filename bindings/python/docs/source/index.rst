PyMongoArrow |release| Documentation
====================================

Overview
--------
**PyMongoArrow** is a `PyMongo <http://pymongo.readthedocs.io/>`_ extension
containing tools for loading `MongoDB <http://www.mongodb.org>`_ query result
sets as  `Apache Arrow <http://arrow.apache.org>`_ tables,
`Pandas <https://pandas.pydata.org>`_ and `NumPy <https://numpy.org>`_ arrays.
PyMongoArrow is the recommended way to materialize MongoDB query result sets as
contiguous-in-memory, typed arrays suited for in-memory analytical processing
applications. This documentation attempts to explain everything you need to
know to use **PyMongoArrow**.

:doc:`installation`
  Instructions on how to get the distribution.

:doc:`quickstart`
  Start here for a quick overview.

:doc:`data_types`
  Data type support with PyMongoArrow.

:doc:`comparison`
  Comparison of using PyMongoArrow versus using PyMongo directly.

:doc:`faq`
  Frequently asked questions.

:doc:`api/index`
  The complete API documentation, organized by module.

:doc:`schemas`
  Important notes about the usage of PyMongoArrow Schemas.

Getting Help
------------
If you're having trouble or have questions about PyMongoArrow, ask your question on
our `MongoDB Community Forum <https://www.mongodb.com/community/forums/tag/python>`_.
Once you get an answer, it'd be great if you could work it back into this
documentation and contribute!

Issues
------
All issues should be reported (and can be tracked / voted for /
commented on) at the main `MongoDB JIRA bug tracker
<http://jira.mongodb.org/browse/PYTHON>`_, in the "Python Driver"
project.

Feature Requests / Feedback
---------------------------
Use our `feedback engine <https://feedback.mongodb.com/forums/924286-drivers>`_
to send us feature requests and general feedback about PyMongoArrow.

Contributing
------------
Contributions to **PyMongoArrow** are encouraged. To contribute, fork the project on
`GitHub <https://github.com/mongodb-labs/mongo-arrow/tree/main/bindings/python>`_
and send a pull request.

See also :doc:`developer/index`.

Changes
-------
See the :doc:`changelog` for a full list of changes to PyMongoArrow.

About This Documentation
------------------------
This documentation is generated using the `Sphinx
<https://www.sphinx-doc.org/en/master/>`_ documentation generator.
The source files for the documentation are located in the *docs/* directory
of the **PyMongoArrow** distribution. To generate the docs locally run the
following command from the root directory of the **PyMongoArrow** source:

.. code-block:: bash

  $ cd docs && make html

Indices and tables
------------------

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

.. toctree::
   :hidden:

   installation
   quickstart
   data_types
   comparison
   faq
   api/index
   changelog
   developer/index
   schemas
