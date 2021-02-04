from setuptools import find_packages, setup
from Cython.Build import cythonize

import os


def get_pymongoarrow_version():
    """Single source the version."""
    version_file = os.path.realpath(os.path.join(
        os.path.dirname(__file__), 'pymongoarrow', 'version.py'))
    version = {}
    with open(version_file) as fp:
        exec(fp.read(), version)
    return version['__version__']


def get_extension_modules():
    modules = cythonize(['pymongoarrow/*.pyx',
                         'pymongoarrow/libbson/*.pyx'])
    for module in modules:
        module.libraries.append('bson-1.0')
    return modules


setup(
    name='pymongoarrow',
    version=get_pymongoarrow_version(),
    packages=find_packages(),
    ext_modules=get_extension_modules(),
    setup_requires=['cython >= 0.29'])
