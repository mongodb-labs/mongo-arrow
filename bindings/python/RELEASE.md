# PyMongoArrow Releases

## Versioning

PyMongoArrow's version numbers follow [semantic
versioning](http://semver.org/): each version number is structured
"major.minor.patch". Patch releases fix bugs, minor releases add
features (and may fix bugs), and major releases include API changes that
break backwards compatibility (and may add features and fix bugs).

In between releases we add .devN to the version number to denote the
version under development. So if we just released 2.3.0, then the
current dev version might be 2.3.1.dev0 or 2.4.0.dev0. When we make the
next release we replace all instances of 2.x.x.devN in the docs with the
new version number.

<https://www.python.org/dev/peps/pep-0440/>

## Release Process

1.  Ensure that the latest commit is passing CI on GitHub Actions as
    expected.

2.  Check JIRA to ensure all the tickets in this version have been
    completed.

3.  Add release notes to `bindings/python/CHANGELOG.md`. Generally just
    summarize/clarify the git log, but you might add some more long form
    notes for big changes.  Replace the `devN` version number with the new version
    number (see  note above in [Versioning](#versioning)). Make sure version number
    is updated in `pymongoarrow/version.py`. Create a PR with the changelog and version
    update.

5.  Run the release workflow: https://github.com/mongodb-labs/mongo-arrow/actions/workflows/release-python.yml.

6.  Make sure the new version appears on
    <https://mongo-arrow.readthedocs.io/en/stable/>. If the new version
    does not show up automatically, trigger a rebuild of "latest":
    <https://readthedocs.org/projects/mongo-arrow/builds/>

7.  Publish the release version in Jira.

8.  Announce the release on:
    <https://www.mongodb.com/community/forums/c/announcements/driver-releases>

9.  Publish the draft GitHub Release for the tag. The title
    should be "PyMongoArrow X.Y.Z", and the description should contain a
    link to the release notes on the the community forum, e.g. "Release
    notes:
    mongodb.com/community/forums/t/pymongoarrow-0-1-1-released/104574."

10. Wait for automated update PR on conda-forge, e.g.:
    <https://github.com/conda-forge/pymongoarrow-feedstock/pull/24>
    Update dependencies if needed.
