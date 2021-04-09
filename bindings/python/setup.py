from setuptools import setup, find_packages
from Cython.Build import cythonize

import os
import subprocess
from sys import platform
import warnings

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
    "pandas>=1.0,<1.2"
]


def append_libbson_flags(module):
    pc_name = 'libbson-1.0'
    module.libraries.append('bson-1.0')

    # https://blog.krzyzanowskim.com/2018/12/05/rpath-what/
    if platform == "darwin":
        module.extra_link_args += ["-rpath", "@loader_path"]

    if 'LIBBSON_INSTALL_DIR' in os.environ:
        libbson_pc_path = os.path.join(
            os.environ['LIBBSON_INSTALL_DIR'], 'lib', 'pkgconfig',
            ".".join([pc_name, 'pc']))
    else:
        libbson_pc_path = pc_name

    status, output = subprocess.getstatusoutput(
        "pkg-config --cflags {}".format(libbson_pc_path))
    if status != 0:
        warnings.warn(output, UserWarning)
    cflags = os.environ.get('CFLAGS', '')
    os.environ['CFLAGS'] = \
        "-D_GLIBCXX_USE_CXX11_ABI=0 {} ".format(output) + cflags

    status, output = subprocess.getstatusoutput(
        "pkg-config --libs {}".format(libbson_pc_path))
    if status != 0:
        warnings.warn(output, UserWarning)
    ldflags = os.environ.get('LDFLAGS', '')
    os.environ['LDFLAGS'] = output + " " + ldflags


def append_arrow_flags(module):
    module.include_dirs.append(np.get_include())
    module.include_dirs.append(pa.get_include())
    module.libraries.extend(pa.get_libraries())
    module.library_dirs.extend(pa.get_library_dirs())

    # https://arrow.apache.org/docs/python/extending.html#example
    if os.name == 'posix':
        module.extra_compile_args.append('-std=c++11')


def get_extension_modules():
    modules = cythonize(['pymongoarrow/*.pyx'])
    for module in modules:
        append_libbson_flags(module)
        append_arrow_flags(module)

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
