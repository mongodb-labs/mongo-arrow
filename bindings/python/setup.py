from setuptools import find_packages, setup
from Cython.Build import cythonize

import os

import numpy as np
import pyarrow as pa


def get_pymongoarrow_version():
    """Single source the version."""
    version_file = os.path.realpath(os.path.join(
        os.path.dirname(__file__), 'pymongoarrow', 'version.py'))
    version = {}
    with open(version_file) as fp:
        exec(fp.read(), version)
    return version['__version__']


def get_extension_modules():
    arrow_modules = cythonize(['pymongoarrow/*.pyx'])
    libbson_modules = cythonize(['pymongoarrow/libbson/*.pyx'])

    for module in libbson_modules:
        module.libraries.append('bson-1.0')

    for module in arrow_modules:
        module.include_dirs.append(np.get_include())
        module.include_dirs.append(pa.get_include())
        module.libraries.extend(pa.get_libraries())
        module.library_dirs.extend(pa.get_library_dirs())

        # https://arrow.apache.org/docs/python/extending.html#example
        if os.name == 'posix':
            module.extra_compile_args.append('-std=c++11')

    return arrow_modules + libbson_modules


setup(
    name='pymongoarrow',
    version=get_pymongoarrow_version(),
    packages=find_packages(),
    ext_modules=get_extension_modules(),
    install_requires=['pyarrow >= 3', 'pymongo >= 3.11,<4'],
    setup_requires=['cython >= 0.29', 'pyarrow >= 3', 'numpy >= 1.16.6'])
