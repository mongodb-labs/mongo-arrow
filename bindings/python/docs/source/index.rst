PyMongoArrow |release| Documentation
====================================

Overview
--------
**PyMongoArrow** is a `PyMongo <http://pymongo.readthedocs.io/>`_ extension
containing tools for loading `MongoDB <http://www.mongodb.org>`_ query result
sets as  `Apache Arrow <http://arrow.apache.org>`_ tables,
`Pandas <https://pandas.pydata.org>`_ and `NumPy <https://numpy.org>`_ arrays.
PyMongoArrow is the recommended way to materialize MongoDB query result sets as
contiguous-in-memory, typed arrays suited for in-memory analytical processing.
This documentation attempts to explain everything you need to know to use
**PyMongoArrow**.

.. todo:: a list of PyMongo's features

:doc:`installation`
  Instructions on how to get the distribution.

:doc:`tutorial`
  Start here for a quick overview.

:doc:`examples/index`
  Examples of how to perform specific tasks.

:doc:`atlas`
  Using PyMongo with MongoDB Atlas.

:doc:`faq`
  Some questions that come up often.

:doc:`api/index`
  The complete API documentation, organized by module.


Getting Help
------------
If you're having trouble or have questions about PyMongoArrow, ask your question on
our `MongoDB Community Forum <https://developer.mongodb.com/community/forums/tags/c/drivers-odms-connectors/7/python-driver>`_.
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
   tutorial
   examples/index
   faq
   api/index
   contributors
   changelog
