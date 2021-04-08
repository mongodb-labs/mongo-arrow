from setuptools import setup, find_packages
from Cython.Build import cythonize

import os
from sys import platform

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


INSTALL_REQUIRES = [
    'pyarrow>=3,<3.1',
    'pymongo>=3.11,<4',
    'numpy>=1.16.6,<2'
]


SETUP_REQUIRES = [
    'cython>=0.29<1',
    'pyarrow>=3,<4',
    'numpy>=1.16.6,<2',
    'setuptools>=41'
]


TESTS_REQUIRE = [
    "pandas"
]


def get_extension_modules():
    modules = cythonize(['pymongoarrow/*.pyx'])

    for module in modules:
        # module.libraries.append('bson-1.0')
        module.include_dirs.append(np.get_include())
        module.include_dirs.append(pa.get_include())
        module.libraries.extend(pa.get_libraries())
        module.library_dirs.extend(pa.get_library_dirs())

        # https://arrow.apache.org/docs/python/extending.html#example
        if os.name == 'posix':
            module.extra_compile_args.append('-std=c++11')

        # https://blog.krzyzanowskim.com/2018/12/05/rpath-what/
        if platform == "darwin":
            module.extra_link_args += ["-rpath", "@loader_path"]

    return modules


with open('README.rst') as f:
    LONG_DESCRIPTION = f.read()


setup(
    name='pymongoarrow',
    packages=find_packages(),
    zip_safe=False,
    package_data={"pymongoarrow": ['*.so', '*.dylib']},
    ext_modules=get_extension_modules(),
    version=get_pymongoarrow_version(),
    python_requires=">=3.6",
    install_requires=INSTALL_REQUIRES,
    setup_requires=SETUP_REQUIRES,
    tests_require=TESTS_REQUIRE,
    description="Tools for using NumPy, Pandas and PyArrow with MongoDB",
    long_description=LONG_DESCRIPTION,
    long_description_content_type='text/x-rst',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: POSIX',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3 :: Only',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: Implementation :: CPython',
        'Topic :: Database'],
    license='Apache License, Version 2.0',
    author="Prashant Mital",
    author_email="mongodb-user@googlegroups.com",
    maintainer="MongoDB, Inc.",
    maintainer_email="mongodb-user@googlegroups.com",
    keywords=["mongo", "mongodb", "pymongo", "arrow", "bson",
              "numpy", "pandas"],
    test_suite="test"
)
