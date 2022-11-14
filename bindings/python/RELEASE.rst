=====================
PyMongoArrow Releases
=====================

Versioning
----------

PyMongoArrow's version numbers follow `semantic versioning <http://semver.org/>`_:
each version number is structured "major.minor.patch". Patch releases fix
bugs, minor releases add features (and may fix bugs), and major releases
include API changes that break backwards compatibility (and may add features
and fix bugs).

In between releases we add .devN to the version number to denote the version
under development. So if we just released 2.3.0, then the current dev
version might be 2.3.1.dev0 or 2.4.0.dev0. When we make the next release we
replace all instances of 2.x.x.devN in the docs with the new version number.

https://www.python.org/dev/peps/pep-0440/

Release Process
---------------

#. Ensure that the latest commit is passing CI on GitHub Actions as expected.

#. Check JIRA to ensure all the tickets in this version have been completed.

#. Add release notes to `doc/source/changelog.rst`. Generally just summarize/clarify
   the git log, but you might add some more long form notes for big changes.

#. Replace the `devN` version number w/ the new version number (see
   note above in `Versioning`_). Make sure version number is updated in
   `pymongoarrow/version.py`. Commit the change and tag the release.
   Immediately bump the version number to `dev0` in a new commit::

     $ # Bump to release version number
     $ git commit -a -m "BUMP <release version number>"
     $ git tag -a "<release version number>" -m "BUMP <release version number>"
     $ # Bump to dev version number
     $ git commit -a -m "BUMP <dev version number>"
     $ git push
     $ git push --tags

#. Download the release assets from the "Python Wheels" Github Workflow, e.g.
https://github.com/mongodb-labs/mongo-arrow/actions/runs/2060477840.

#. Upload all the release packages to PyPI with twine::

     $ python3 -m twine upload dist/*

#. Make sure the new version appears on https://mongo-arrow.readthedocs.io/en/latest/. If the
   new version does not show up automatically, trigger a rebuild of "latest":
   https://readthedocs.org/projects/mongo-arrow/builds/

#. Publish the release version in Jira.

#. Announce the release on:
   https://www.mongodb.com/community/forums/c/announcements/driver-releases

#. Create a GitHub Release for the tag using https://github.com/mongodb/mongo-arrow/releases/new.
   The title should be "PyMongoArrow X.Y.Z", and the description should contain
   a link to the release notes on the the community forum, e.g.
   "Release notes: mongodb.com/community/forums/t/pymongoarrow-0-1-1-released/104574."
