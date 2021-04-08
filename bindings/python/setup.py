from setuptools import setup, find_packages
from Cython.Build import cythonize

import glob
import os
import shutil
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
    'numpy>=1.16.6,<2',
]


TESTS_REQUIRE = [
    "pandas"
]


def vendor_libbson():
    if os.environ.get('USE_STATIC_LIBBSON') == '1':
        pc_name = 'libbson-static-1.0'
    else:
        pc_name = 'libbson-1.0'

    if 'LIBBSON_INSTALL_DIR' in os.environ:
        libbson_pc_path = os.path.join(
            os.environ['LIBBSON_INSTALL_DIR'], 'lib', 'pkgconfig',
            ".".join([pc_name, 'pc']))
    else:
        libbson_pc_path = pc_name

    # Set CFLAGS and LDFLAGS for build
    for var, cmd in [
            ("CFLAGS", "pkg-config --cflags {}".format(libbson_pc_path)),
            ("LDFLAGS", "pkg-config --libs {}".format(libbson_pc_path))]:
        status, output = subprocess.getstatusoutput(cmd)
        if status != 0:
            warnings.warn(output, UserWarning)
        else:
            os.environ[var] = output

    # Copy libbson SO if not linking statically
    if os.environ.get('USE_STATIC_LIBBSON') != '1':
        status, output = subprocess.getstatusoutput(
            "pkg-config --libs-only-L {}".format(libbson_pc_path))
        if status != 0:
            warnings.warn(
                "unable to vendor libbson - not found", UserWarning)

        basepath = output.lstrip('-L')
        if platform == 'darwin':
            so_ext = '.dylib'
        elif platform == 'linux':
            so_ext = '.so'
        else:
            warnings.warn(
                "unable to vendor libbson - unsupported platform", UserWarning)
            return
        libpath = os.path.join(basepath, "libbson-1.0.0{}*".format(so_ext))
        libs = glob.glob(libpath)
        target = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), 'pymongoarrow')
        for lib in libs:
            shutil.copy(lib, target)


def append_libbson_flags(module):
    if os.environ.get('USE_STATIC_LIBBSON') != '1':
        # Make our cython extensions load the libbson library embedded in pymongoarrow/
        # https://blog.krzyzanowskim.com/2018/12/05/rpath-what/
        if platform == "darwin":
            module.extra_link_args += ["-rpath", "@loader_path"]
        # https://nehckl0.medium.com/creating-relocatable-linux-executables-by-setting-rpath-with-origin-45de573a2e98
        elif platform == "linux":
            module.extra_link_args += ["-Wl,-rpath,$ORIGIN"]


def append_arrow_flags(module):
    module.include_dirs.append(np.get_include())
    module.include_dirs.append(pa.get_include())
    module.libraries.extend(pa.get_libraries())
    module.library_dirs.extend(pa.get_library_dirs())

    if os.name == 'posix':
        # https://arrow.apache.org/docs/python/extending.html#example
        module.extra_compile_args.append('-std=c++11')
        # https://uwekorn.com/2019/09/15/how-we-build-apache-arrows-manylinux-wheels.html
        module.extra_compile_args.append("-D_GLIBCXX_USE_CXX11_ABI=0")
        module.extra_link_args.append("-D_GLIBCXX_USE_CXX11_ABI=0")


def ensure_pyarrow_linkable():
    # https://arrow.apache.org/docs/python/extending.html#building-extensions-against-pypi-wheels
    pa.create_library_symlinks()


def get_extension_modules():
    modules = cythonize(['pymongoarrow/*.pyx'])
    vendor_libbson()
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
