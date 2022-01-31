from multiprocessing import Value
import shutil
from setuptools import setup

import glob
import os
import sys
import subprocess
import warnings

# We use posix paths everywhere so bash can parse the paths
# on Windows.
from pathlib import PurePosixPath
from sys import platform


HERE = os.path.abspath(os.path.dirname(__file__))
BUILD_DIR = PurePosixPath(HERE) / 'pymongoarrow'
IS_WIN = platform == 'win32'
LIBBSON_NAME = 'libbson-1.0'

# Find and copy the binary arrow files, unless
# MONGO_NO_COPY_ARROW_LIB is set (for instance in a conda build).
# Wheels are meant to be self-contained, per PEP 513.
# https://www.python.org/dev/peps/pep-0513/#id40
# Conda has the opposite philosphy, where libraries are meant to be
# shared.  For instance, there is an arrow-cpp library available on conda-forge
# that provides the libarrow files.
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


def get_bson_install_dir():
    install_dir_env = os.environ.get('LIBBSON_INSTALL_DIR')
    if install_dir_env:
        install_dir = PurePosixPath(install_dir_env)
    else:
        install_dir = None
    return install_dir


def append_libbson_flags_win(module):
    install_dir = get_bson_install_dir()
    err_msg = f'Could not find the compiled libbson in {install_dir}'

    # If no install dir is given, try looking for
    # an installed python package (e.g. libbson from conda-forge).
    if not install_dir:
        install_dir = PurePosixPath(sys.base_prefix) / 'Library'

    # Handle copying the dynamic library if applicable.
    if COPY_LIBBSON:
        dll_file = install_dir / 'bin' / 'bson-1.0.dll'
        if not os.path.exists(dll_file):
            raise Value(err_msg)
        shutil.copy(dll_file, BUILD_DIR)


    # Find the static library, and explicity add it to the linker.
    lib_glob = glob.glob(str(install_dir / 'lib*' / 'bson-1.0.lib'))
    if len(lib_glob) != 1:
        raise ValueError(err_msg)

    module.extra_link_args = [lib_glob[0]]
    include_dir = install_dir / 'include' / LIBBSON_NAME
    module.include_dirs.append(str(include_dir))


def append_libbson_flags(module):
    pc_path = LIBBSON_NAME
    install_dir = get_bson_install_dir()
    version = None
    err_msg = f'Could not find "{LIBBSON_NAME}" library in {install_dir}'

    # If no install dir is given, try looking for a system package or
    # an installed python package (e.g. libbson from conda-forge).
    if not install_dir:
        # Prefer system-installed libbson, falling back to the python
        # local version.
        version = query_pkgconfig(f"pkg-config --version {LIBBSON_NAME}")
        if not version:
            install_dir = PurePosixPath(sys.base_prefix)

    if install_dir and not version:
        pc_glob = glob.glob(str(install_dir / 'lib*' / 'pkgconfig'))
        if not pc_glob:
            raise ValueError(err_msg)
        pc_path = PurePosixPath(pc_glob[0]) / f'{LIBBSON_NAME}.pc'
        version = query_pkgconfig(f"pkg-config --version {pc_path}")

    if not version:
        raise ValueError(err_msg)

    # Handle copying the dynamic libary if applicable.
    if install_dir and COPY_LIBBSON:
        if platform == "darwin":
            lib_file = "libbson-1.0.0.dylib"
        elif platform == "linux":
            lib_file = "libbson-1.0.so.0"
        else:
            raise RuntimeError(f'Unsupported Platform {platform}')

        lib_glob = glob.glob(str(install_dir / 'lib*' / lib_file))
        if len(lib_glob) != 1:
            raise ValueError(err_msg)
        shutil.copy(lib_glob[0], BUILD_DIR)

    cflags = query_pkgconfig(f"pkg-config --cflags {pc_path}")
    if cflags:
        orig_cflags = os.environ.get('CFLAGS', '')
        os.environ['CFLAGS'] = cflags + " " + orig_cflags

    ldflags = query_pkgconfig(f"pkg-config --libs {pc_path}")
    if ldflags:
        orig_ldflags = os.environ.get('LDFLAGS', '')
        os.environ['LDFLAGS'] = ldflags + " " + orig_ldflags

    # https://cython.readthedocs.io/en/latest/src/tutorial/external.html#dynamic-linking
    # Strip whitespace to avoid weird linker failures on manylinux images
    lnames = query_pkgconfig(f"pkg-config --libs-only-l {pc_path}")
    if lnames:
        libnames = [lname.lstrip('-l').strip() for lname in lnames.split()]
        module.libraries.extend(libnames)


def get_arrow_lib():
    # Handle the arrow library files manually.
    # Alternative to using pyarrow.create_library_symlinks().
    # You can use MONGO_LIBARROW_DIR to explicitly set the location of the
    # arrow libraries (for instance in a conda build).
    # We first check for an unmodified path to the library,
    # then look for a library file with a version modifier, e.g. libarrow.600.dylib.
    import pyarrow as pa
    arrow_lib = os.environ.get('MONGO_LIBARROW_DIR', pa.get_library_dirs()[0])
    return PurePosixPath(arrow_lib)


def append_arrow_flags_win(module):
    import numpy as np
    import pyarrow as pa

    arrow_lib = get_arrow_lib()
    module.include_dirs.append(np.get_include())
    module.include_dirs.append(pa.get_include())

    # Find the appropriate library file and optionally copy it locally.
    for name in pa.get_libraries():
        if COPY_LIBARROW:
            lib_file = arrow_lib / f'{name}.dll'
            if not os.path.exists(lib_file):
                raise ValueError('Could not find compiled arrow library')
            shutil.copy(lib_file, BUILD_DIR)
        lib_file = arrow_lib / f'{name}.lib'
        module.extra_link_args.append(str(lib_file))
        continue


def append_arrow_flags(module):
    import numpy as np
    import pyarrow as pa

    arrow_lib = get_arrow_lib()

    module.extra_compile_args.append("-isystem" + pa.get_include())
    module.extra_compile_args.append("-isystem" + np.get_include())
    # Arrow's manylinux{2010, 2014} binaries are built with gcc < 4.8 which predates CXX11 ABI
    # - https://uwekorn.com/2019/09/15/how-we-build-apache-arrows-manylinux-wheels.html
    # - https://arrow.apache.org/docs/python/extending.html#example
    if "std=" not in os.environ.get("CXXFLAGS", ""):
        module.extra_compile_args.append("-std=c++11")
        module.extra_compile_args.append("-D_GLIBCXX_USE_CXX11_ABI=0")

    if platform == "darwin":
        exts = ['.dylib', '.*.dylib']
    elif platform == 'linux':
        exts = ['.so', '.so.*']
    else:
        raise Value(f'Unsupported platform {platform}')

    # Find the appropriate library file and optionally copy it locally.
    for name in pa.get_libraries():
        for ext in exts:
            files = glob.glob(str(arrow_lib / f'lib{name}{ext}'))
            if not files:
                continue
            path = PurePosixPath(files[0])
            if COPY_LIBARROW:
                shutil.copy(path, BUILD_DIR)
            module.extra_link_args.append(str(BUILD_DIR / path.name))
            break


def append_extra_link_args(module):
    # Ensure our Cython extension can dynamically link to libraries
    # - https://blog.krzyzanowskim.com/2018/12/05/rpath-what/
    # - https://nehckl0.medium.com/creating-relocatable-linux-executables-by-setting-rpath-with-origin-45de573a2e98
    if platform == "darwin":
        module.extra_link_args += ["-rpath", "@loader_path"]
    elif platform == 'linux':
        module.extra_link_args += ["-Wl,-rpath,$ORIGIN"]


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
        if IS_WIN:
            append_libbson_flags_win(module)
            append_arrow_flags_win(module)
        else:
            append_libbson_flags(module)
            append_arrow_flags(module)
            append_extra_link_args(module)
    return modules


setup(ext_modules=get_extension_modules())
