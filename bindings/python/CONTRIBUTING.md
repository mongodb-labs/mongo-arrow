# Contributing to PyMongoArrow

Contributions to PyMongoArrow are always encouraged. Contributions can be as simple
as minor tweaks to the documentation. Please read these guidelines
before sending a pull request.

## Bugfixes and New Features

Before starting to write code, look for existing [tickets in our JIRA project](https://jira.mongodb.org/browse/INTPYTHON)
or [create one](https://jira.mongodb.org/browse/INTPYTHON) for your specific issue or
feature request. That way you avoid working on something that might not
be of interest or that has already been addressed.

## Supported Interpreters

PyMongoArrow supports CPython 3.10+. Language features not
supported by all interpreters can not be used.

## Style Guide

PyMongoArrow uses [Ruff](https://docs.astral.sh/ruff/) for formatting and linting, with a line length of 100.

## General Guidelines

-   Avoid backward breaking changes if at all possible.
-   Write inline documentation for new classes and methods.
-   Write tests and make sure they pass (make sure you have a mongod
    running on the default port, then execute `just test` from the cmd
    line to run the test suite).

## Authoring a Pull Request

**Our Pull Request Policy is based on this** [Code Review Developer
Guide](https://google.github.io/eng-practices/review)

The expectation for any code author is to provide all the context needed
in the space of a pull request for any engineer to feel equipped to
review the code. Depending on the type of change, do your best to
highlight important new functions or objects you've introduced in the
code; think complex functions or new abstractions. Whilst it may seem
like more work for you to adjust your pull request, the reality is your
likelihood for getting a review sooner shoots up.

**Self Review Guidelines to follow**

-   If the PR is too large, split it if possible.

    - Use 250 lines of code (excluding test data and config changes) as a
            rule-of-thumb.

     - Moving and changing code should be in separate PRs or commits.

        -   Moving: Taking large code blobs and transplanting
            them to another file. There\'s generally no (or very
            little) actual code changed other than a cut and
            paste. It can even be extended to large deletions.
        -   Changing: Adding code changes (be that refactors or
            functionality additions/subtractions).
        -   These two, when mixed, can muddy understanding and
            sometimes make it harder for reviewers to keep track
            of things.

-   Prefer explaining with code comments instead of PR comments.

**Provide background**

-   The PR description and linked tickets should answer the "what" and
    "why" of the change. The code change explains the "how".

**Follow the Template**

-   Please do not deviate from the template we make; it is there for a
    lot of reasons. If it is a one line fix, we still need to have
    context on what and why it is needed.

-   If making a versioning change, please let that be known. See examples below:

    -   `versionadded:: 3.11`
    -   `versionchanged:: 3.5`

### AI-Generated Contributions Policy

#### Our Stance

We only accept pull requests that are authored and submitted by human contributors who fully understand the changes they are proposing. Pull requests that are not clearly owned and understood by a human contributor may be closed. **All contributions must be submitted, reviewed, and understood by human contributors.**

##### Why This Policy Exists

At MongoDB, we understand the power and prevalence of AI tools in software development. With that being said, many MongoDB libraries are foundational tools used in production systems worldwide. The nature of these libraries requires:

- **Deep domain expertise**: PyMongoArrow bridges MongoDB and the Apache Arrow ecosystem. BSON serialization, PyArrow schema inference, Polars integration, and Cython bindings require an understanding that AI alone cannot substantiate.

- **Long-term maintainability**: Contributors need to be able to explain *why* code is written a certain way, explain design decisions, and be available to iterate on their contributions.

- **Security responsibility**: Data type handling, schema inference, and memory management cannot be left to probabilistic code generation.

##### What This Means for Contributors

**Required:**

- Full understanding of every line of code you submit
- Ability to explain and defend your implementation choices
- Willingness to iterate and maintain your contributions

**Encouraged:**

- Using AI assistants as learning tools to understand concepts
- IDE autocomplete features that suggest standard patterns
- AI help for brainstorming approaches (but write the code yourself)
- Writing code using AI tools, reviewing each line and revising code as necessary.

**Not allowed:**

- Submitting PRs generated solely by AI tools
- Copy-pasting AI-generated code without full understanding

##### Disclosure

If you used AI assistance in any way during your contribution, please disclose what the AI assistant was used for in your PR description. We would love to know what tools developers have found useful in iterating in their day to day.

##### Questions?

If you're unsure whether your contribution complies with this policy, please ask for guidance within the scope of the PR and clarify any uncertainty. We're happy to guide contributors toward successful contributions.

---

*This policy helps us maintain the reliability, security, and trustworthiness that production applications depend on. Thank you for understanding and for contributing thoughtfully to PyMongoArrow.*
# Installing from source

## System Requirements

On macOS, you need a working modern XCode installation with the XCode
Command Line Tools. Additionally, you need CMake and pkg-config:

``` bash
$ xcode-select --install
$ brew install cmake
$ brew install pkg-config
```

On Linux, installation requires gcc 12, CMake and pkg-config.

Windows is not yet supported.

## Environment Setup

First, clone the mongo-arrow git repository:

```bash
git clone https://github.com/mongodb-labs/mongo-arrow.git
cd mongo-arrow/bindings/python
```

Ensure you have [just](https://just.systems/man/en/introduction.html) and [uv](https://docs.astral.sh/uv/getting-started/installation/) installed.
Then run:

```bash
just install
```

### libbson

PyMongoArrow uses
[libbson](http://mongoc.org/libbson/current/index.html). Detailed
instructions for building/installing `libbson` can be found
[here](http://mongoc.org/libmongoc/1.21.0/installing.html#installing-the-mongodb-c-driver-libmongoc-and-bson-library-libbson).

You can either use a system-provided version of `libbson` that is
properly configured for use with `pkg-config`, or use the provided
`build-libbson.sh` script to build it:

``` bash
just build-libbson
```

On macOS, users can install the latest `libbson` using Homebrew:

``` bash
$ brew install mongo-c-driver
```

Conda users can install `libbson` as follows:

``` bash
$ conda install --channel conda-forge libbson pkg-config
```

The minimum required version is listed in `pymongoarrow/version.py`. If
you try to build with a lower version a `ValueError` will be raised.

Our minimum supported major version is 1.x, and that is what we include in our wheels.
In order to build with `bson2`, you can `export LIBBSON_VERSION=2.<minor>.<patch>` before
running `just build-libbson` or building the library.

## Build

Typically we will use the provided `just` scripts and will not build
directly, but you can build and test in the created virtualenv.

In the previously created virtualenv, to install PyMongoArrow and its
test dependencies in editable mode:

```bash
pip install -v -e ".[test]"
```

If you built libbson using the `build-libbson` script then use the same
`LIBBSON_INSTALL_DIR` as above:

> (pymongoarrow) \$ LIBBSON_INSTALL_DIR=\$(pwd)/libbson pip install -v
> -e ".\[test\]"

## Test

To run the test suite, you will need a MongoDB instance running on
`localhost` using port `27017`. To run the entire test suite, do:

```bash
just test
```

or, if not using `just`:

> (pymongoarrow) \$ pytest

## Running Linters

PyMongoArrow uses [pre-commit](https://pypi.org/project/pre-commit/) for
managing linting of the codebase. `pre-commit` performs various checks
on all files in PyMongoArrow and uses tools that help follow a
consistent code style within the codebase.

To set up `pre-commit` locally, run:

```bash
just install
```

To run `pre-commit` manually, run:

```bash
just lint
```

## Running Benchmarks

### System Requirements

To run the benchmarks, you need the [asv](https://pypi.org/project/asv/)
package, which can then be invoked like so:

```bash
asv run --strict --python=`which python`
```

or you can run with just as:

```bash
just benchmark
```
