import shutil
from setuptools import setup

import glob
import os
import re
import subprocess
from sys import platform
import warnings


HERE = os.path.abspath(os.path.dirname(__file__))
BUILD_DIR = os.path.join(HERE, 'pymongoarrow')

# Find and copy the binary arrow files, unless
# MONGO_NO_COPY_ARROW_LIB is set (for instance in a conda build).
COPY_LIBARROW = not os.environ.get("MONGO_NO_COPY_LIBARROW", False)

# Find and copy the binary libbson file, unless
# MONGO_NO_COPY_LIBBSON is set (for instance in a conda build).
COPY_LIBBSON = not os.environ.get("MONGO_NO_COPY_LIBBSON", False)


def query_pkgconfig(cmd):
    status, output = subprocess.getstatusoutput(cmd)
    if status != 0:
        warnings.warn(output, UserWarning)
        return None
    return output


def append_libbson_flags(module):
    pc_path = 'libbson-1.0'
    install_dir = os.environ.get('MONGO_LIBBSON_DIR')
    if install_dir:
        # Handle the copy-able library file if applicable.
        if COPY_LIBBSON:
            if platform == "darwin":
                lib_file = "libbson-1.0.0.dylib"
            elif platform == "linux":
                lib_file = "libbson-1.0.so.0"
            else:  # windows
                lib_file = 'bson-1.0.dll'
            lib_dir = 'bin' if os.name == 'nt' else 'lib*'
            lib_dir = glob.glob(os.path.join(install_dir, lib_dir))
            if lib_dir:
                lib_file = os.path.join(lib_dir[0], lib_file)
                if os.path.exists(lib_file):
                    shutil.copy(lib_file, BUILD_DIR)

        # Find the linkable library file, and explicity add it to the linker if on Windows.
        lib_dirs = glob.glob(os.path.join(install_dir, "lib*"))
        if len(lib_dirs) != 1:
            warnings.warn(f"Unable to locate libbson in {install_dir}")
        else:
            lib_dir = lib_dirs[0]
            if os.name == 'nt':
                lib_path = os.path.join(lib_dir, 'bson-1.0.lib').replace(os.sep, '/')
                if os.path.exists(lib_path):
                    module.extra_link_args = [lib_path]
                    include_dir = os.path.join(install_dir, 'include', 'libbson-1.0').replace(os.sep, '/')
                    module.include_dirs.append(include_dir)
                else:
                    raise ValueError('We require a MONGO_LIBBSON_DIR with a compiled library on Windows')
            pc_path = os.path.join(
                install_dir, lib_dir, 'pkgconfig', 'libbson-1.0.pc')

    elif os.name == 'nt':
        raise ValueError('We require a MONGO_LIBBSON_DIR with a compiled library on Windows')

    if os.name == 'nt':
        # We have either added the library file or raised an error, so return.
        return

    lnames = query_pkgconfig("pkg-config --libs-only-l {}".format(pc_path))
    if not lnames:
        raise ValueError(f'Could not find "{pc_path}" library')

    cflags = query_pkgconfig("pkg-config --cflags {}".format(pc_path))
    if cflags:
        orig_cflags = os.environ.get('CFLAGS', '')
        os.environ['CFLAGS'] = cflags + " " + orig_cflags

    ldflags = query_pkgconfig("pkg-config --libs {}".format(pc_path))
    if ldflags:
        orig_ldflags = os.environ.get('LDFLAGS', '')
        os.environ['LDFLAGS'] = ldflags + " " + orig_ldflags

    # Ensure our Cython extension can dynamically link to vendored libbson
    # - https://blog.krzyzanowskim.com/2018/12/05/rpath-what/
    # - https://nehckl0.medium.com/creating-relocatable-linux-executables-by-setting-rpath-with-origin-45de573a2e98
    if platform == "darwin":
        module.extra_link_args += ["-rpath", "@loader_path"]
    elif platform == 'linux':
        module.extra_link_args += ["-Wl,-rpath,$ORIGIN"]

    # https://cython.readthedocs.io/en/latest/src/tutorial/external.html#dynamic-linking
    # Strip whitespace to avoid weird linker failures on manylinux images
    libnames = [lname.lstrip('-l').strip() for lname in lnames.split()]
    module.libraries.extend(libnames)


def append_arrow_flags(module):
    import numpy as np
    import pyarrow as pa

    module.include_dirs.append(np.get_include())
    module.include_dirs.append(pa.get_include())
    module.library_dirs.extend(pa.get_library_dirs())

    # Handle the arrow library files manually.
    # Alternative to using pyarrow.create_library_symlinks().
    # You can use MONGO_LIBARROW_DIR to explicitly set the location of the 
    # arrow libraries (for instance in a conda build).
    # We first check for an unmodified path to the library,
    # then look for a library file with a version modifier, e.g. libarrow.600.dylib.
    arrow_lib = os.environ.get('MONGO_LIBARROW_DIR', pa.get_library_dirs()[0])
    if platform == "darwin":
        exts = ['.dylib', '.*.dylib']
    elif platform == 'linux':
        exts = ['.so', '.so.*']

    # Find the appropriate library file and optionally copy it locally.
    for name in pa.get_libraries():
        if os.name == 'nt':
            lib_file = os.path.join(arrow_lib, f'{name}.dll')
            if not os.path.exists(lib_file):
                raise ValueError('Could not find compiled arrow library')
            if COPY_LIBARROW:
                shutil.copy(lib_file, BUILD_DIR)
            lib_file = os.path.join(arrow_lib, f'{name}.lib')
            module.extra_link_args.append(lib_file)
            continue

        for ext in exts:
            files = glob.glob(os.path.join(arrow_lib, f'lib{name}{ext}'))
            if not files:
                continue
            path = files[0]
            if COPY_LIBARROW:
                shutil.copy(path, BUILD_DIR)
                path = os.path.join(BUILD_DIR, os.path.basename(path))
            
            module.extra_link_args.append(path)
            break
            
    # Arrow's manylinux{2010, 2014} binaries are built with gcc < 4.8 which predates CXX11 ABI
    # - https://uwekorn.com/2019/09/15/how-we-build-apache-arrows-manylinux-wheels.html
    # - https://arrow.apache.org/docs/python/extending.html#example
    if os.name != "nt":
        module.define_macros.append(("_GLIBCXX_USE_CXX11_ABI", "0"))
        if os.name == 'posix':
            module.extra_compile_args.append('-std=c++11')


def get_extension_modules():
    # This change is needed in order to allow setuptools to import the
    # library to obtain metadata information outside of a build environment.
    try:
        from Cython.Build import cythonize
    except ImportError:
        warnings.warn("Cannot compile native C code, because of a missing build dependency")
        return []
    modules = cythonize(['pymongoarrow/*.pyx'])
    for module in modules:
        append_libbson_flags(module)
        append_arrow_flags(module)

    return modules


setup(ext_modules=get_extension_modules())
