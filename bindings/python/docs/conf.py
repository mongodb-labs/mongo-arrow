# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.abspath(".."))  # noqa: PTH100

# -- Project information -----------------------------------------------------

project = "PyMongoArrow"
copyright = "MongoDB, Inc. 2021-present. MongoDB, Mongo, and the leaf logo are registered trademarks of MongoDB, Inc"  # noqa:E501
author = "Prashant Mital"
html_show_sphinx = False

HERE = Path(__file__).absolute().parent
version_file = HERE / "../pymongoarrow/version.py"
version_data = {}
with version_file.open() as vf:
    exec(vf.read(), {}, version_data)  # noqa:S102
version = version_data["__version__"]
release = version

# The name of the Pygments (syntax highlighting) style to use.
pygments_style = "sphinx"

# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
# Add any Sphinx extension module names here, as strings. They can be extensions
# coming with Sphinx (named 'sphinx.ext.*') or your custom ones.
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.doctest",
    "sphinx.ext.coverage",
    "sphinx.ext.todo",
    "sphinx.ext.intersphinx",
]

# Add optional extensions
try:
    import sphinxcontrib.shellcheck  # noqa: F401

    extensions += ["sphinxcontrib.shellcheck"]
except ImportError:
    pass

# Add any paths that contain templates here, relative to this directory.
templates_path = ["_templates"]

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = []

# If true, the current module name will be prepended to all description
# unit titles (such as .. function::).
add_module_names = True

# The name of the Pygments (syntax highlighting) style to use.
pygments_style = "sphinx"

# -- Options for extensions ----------------------------------------------------
autoclass_content = "init"

autodoc_typehints = "description"

# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
try:
    import furo  # noqa: F401

    html_theme = "furo"
except ImportError:
    # Theme gratefully vendored from CPython source.
    html_theme = "pydoctheme"
    html_theme_path = ["."]
    html_theme_options = {"collapsiblesidebar": True, "googletag": False}

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = []

# These paths are either relative to html_static_path
# or fully qualified paths (eg. https://...)
# Note: html_js_files was added in Sphinx 1.8.
html_js_files = [
    "delighted.js",
]

# Output file base name for HTML help builder.
htmlhelp_basename = "PyMongoArrow" + release.replace(".", "_")

# intersphinx_mapping = {
#     "pyarrow": ("https://arrow.apache.org/docs/", None),
#     "pandas": ("https://pandas.pydata.org/docs/", None),
#     "numpy": ("https://numpy.org/doc/1.20/", None),
#     "pymongo": ("https://pymongo.readthedocs.io/en/stable/", None),
#     "bson": ("https://pymongo.readthedocs.io/en/stable/", None),
# }
