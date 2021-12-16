import shutil
from setuptools import setup

import glob
import os
import re
import subprocess
from sys import platform
import warnings


HERE = os.path.abspath(os.path.dirname(__file__))

if platform not in ('linux', 'darwin'):
    raise RuntimeError("Unsupported plaform {}".format(platform))


def query_pkgconfig(cmd):
    status, output = subprocess.getstatusoutput(cmd)
    if status != 0:
        warnings.warn(output, UserWarning)
        return None
    return output


def append_libbson_flags(module):
    pc_path = 'libbson-1.0'
    install_dir = os.environ.get('LIBBSON_INSTALL_DIR')
    if install_dir:
        libdirs = glob.glob(os.path.join(install_dir, "lib*"))
        if len(libdirs) != 1:
            warnings.warn("Unable to locate {}".format('libbson-1.0.pc'))
        else:
            libdir = libdirs[0]
            pc_path = os.path.join(
                install_dir, libdir, 'pkgconfig', 'libbson-1.0.pc')

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
    lnames = lnames.split()
    # Strip whitespace to avoid weird linker failures on manylinux images
    libnames = [lname.lstrip('-l').strip() for lname in lnames]
    module.libraries.extend(libnames)


def append_arrow_flags(module):
    import numpy as np
    import pyarrow as pa

    module.include_dirs.append(np.get_include())
    module.include_dirs.append(pa.get_include())
    module.library_dirs.extend(pa.get_library_dirs())

    # Handle the arrow library files manually.
    # Alternative to using pyarrow.create_library_symlinks().
    # You can use MONGO_ARROW_LIBDIR to explicitly set the location of the 
    # arrow libraries (for instance in a conda build).
    # We first check for an unmodified path to the library,
    # then look for a library file with a version modifier, e.g. libarrow.600.dylib.
    arrow_lib = os.environ.get('MONGO_ARROW_LIBDIR', pa.get_library_dirs()[0])
    if platform == "darwin":
        exts = ['.dylib', '.*.dylib']
    elif platform == 'linux':
        exts = ['.so', '.so.*']

    # Find the appropriate library file and optionally copy it locally.
    for name in pa.get_libraries():
        for ext in exts:
            files = glob.glob(os.path.join(arrow_lib, f'lib{name}{ext}'))
            if not files:
                continue
            path = files[0]
    
            # You can use MONGO_NO_COPY_ARROW_LIB to avoid copying the arrow library
            # files to the build directory (for instance in a conda build).
            if not os.environ.get("MONGO_NO_COPY_ARROW_LIB", False):
                build_dir = os.path.join(HERE, 'pymongoarrow')
                shutil.copy(path, build_dir)
                path = os.path.join(build_dir, os.path.basename(path))
            module.extra_link_args.append(path)
            break

    # Arrow's manylinux{2010, 2014} binaries are built with gcc < 4.8 which predates CXX11 ABI
    # - https://uwekorn.com/2019/09/15/how-we-build-apache-arrows-manylinux-wheels.html
    # - https://arrow.apache.org/docs/python/extending.html#example
    module.define_macros.append(("_GLIBCXX_USE_CXX11_ABI", "0"))
    if os.name == 'posix':
        module.extra_compile_args.append('-std=c++11')


def get_extension_modules():
    # This change is needed in order to allow setuptools to import the
    # library to obtain metadata information outside of a build environment.
    try:
        from Cython.Build import cythonize
    except ImportError:
        return []
    modules = cythonize(['pymongoarrow/*.pyx'])
    for module in modules:
        append_libbson_flags(module)
        append_arrow_flags(module)

    return modules


setup(ext_modules=get_extension_modules())
