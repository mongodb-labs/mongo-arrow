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
    notes for big changes.

4.  Replace the `devN` version number w/ the new version number (see
    note above in [Versioning](#versioning)). Make sure version number
    is updated in `pymongoarrow/version.py`. Commit the change and tag
    the release. Immediately bump the version number to `dev0` in a new
    commit:

        $ # Bump to release version number
        $ git commit -a -m "BUMP <release version number>"
        $ git tag -a "<release version number>" -m "BUMP <release version number>"
        $ # Bump to dev version number
        $ git commit -a -m "BUMP <dev version number>"
        $ git push
        $ git push --tags

5.  Pushing a tag will trigger the release process on GitHub Actions
    that will require a member of the team to authorize the deployment.
    Navigate to
    <https://github.com/mongodb-labs/mongo-arrow/actions/workflows/release-python.yml>
    and wait for the publish to complete.

6.  Make sure the new version appears on
    <https://mongo-arrow.readthedocs.io/en/stable/>. If the new version
    does not show up automatically, trigger a rebuild of "latest":
    <https://readthedocs.org/projects/mongo-arrow/builds/>

7.  Publish the release version in Jira.

8.  Announce the release on:
    <https://www.mongodb.com/community/forums/c/announcements/driver-releases>

9.  Create a GitHub Release for the tag using
    <https://github.com/mongodb/mongo-arrow/releases/new>. The title
    should be "PyMongoArrow X.Y.Z", and the description should contain a
    link to the release notes on the the community forum, e.g. "Release
    notes:
    mongodb.com/community/forums/t/pymongoarrow-0-1-1-released/104574."

10. Wait for automated update PR on conda-forge, e.g.:
    <https://github.com/conda-forge/pymongoarrow-feedstock/pull/24>
    Update dependencies if needed.
