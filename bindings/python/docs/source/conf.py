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
import os.path as p


# -- Project information -----------------------------------------------------

project = 'PyMongoArrow'
copyright = 'MongoDB, Inc. 2021-present. MongoDB, Mongo, and the leaf logo are registered trademarks of MongoDB, Inc'
author = 'Prashant Mital'
html_show_sphinx = False

version_file = p.abspath(p.join("../../", "pymongoarrow/version.py"))
version_data = {}
with open(version_file, 'r') as vf:
    exec(vf.read(), {}, version_data)
version = version_data['__version__']
release = version

# The name of the Pygments (syntax highlighting) style to use.
pygments_style = 'sphinx'

# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.intersphinx'
]

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = []


# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = 'pydoctheme'
html_theme_path = ["."]
html_theme_options = {
    'collapsiblesidebar': True,
    'googletag': False
}

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = []

# These paths are either relative to html_static_path
# or fully qualified paths (eg. https://...)
# Note: html_js_files was added in Sphinx 1.8.
html_js_files = [
    'delighted.js',
]

# Output file base name for HTML help builder.
htmlhelp_basename = 'PyMongoArrow' + release.replace('.', '_')

intersphinx_mapping = {
    'pyarrow': ('https://arrow.apache.org/docs/', None),
    'pandas': ('https://pandas.pydata.org/docs/', None),
    'numpy': ('https://numpy.org/doc/1.20/', None),
    'pymongo': ('https://pymongo.readthedocs.io/en/stable/', None)
}
