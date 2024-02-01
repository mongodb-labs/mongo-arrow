import glob
import os
import shutil
import subprocess
import warnings
from sys import platform

from setuptools import setup

HERE = os.path.abspath(os.path.dirname(__file__))
BUILD_DIR = os.path.join(HERE, "pymongoarrow")
IS_WIN = platform == "win32"

# Find and copy the binary libbson file, unless
# MONGO_NO_COPY_LIBBSON is set (for instance in a conda build).
COPY_LIBBSON = not os.environ.get("MONGO_NO_COPY_LIBBSON", "")

# Whether to create libarrow symlinks on posix systems.
CREATE_LIBARROW_SYMLINKS = os.environ.get("MONGO_CREATE_LIBARROW_SYMLINKS", "1")

# Set a default value for MACOSX_DEPLOYMENT_TARGET.
os.environ.setdefault("MACOSX_DEPLOYMENT_TARGET", "10.15")


def query_pkgconfig(cmd):
    status, output = subprocess.getstatusoutput(cmd)
    if status != 0:
        warnings.warn(output, UserWarning)
        return None
    return output


def get_min_libbson_version():
    version_ns = {}
    here = os.path.dirname(__file__)
    version_py = os.path.join(here, "pymongoarrow", "version.py")
    with open(version_py) as f:
        exec(compile(f.read(), version_py, "exec"), version_ns)
    return version_ns["_MIN_LIBBSON_VERSION"]


def append_libbson_flags(module):
    pc_path = "libbson-1.0"
    install_dir = os.environ.get("LIBBSON_INSTALL_DIR")
    if install_dir:
        install_dir = os.path.abspath(install_dir)

        # Handle the copy-able library file if applicable.
        if COPY_LIBBSON:
            if platform == "darwin":
                lib_file = "libbson-1.0.0.dylib"
            elif platform == "linux":
                lib_file = "libbson-1.0.so.0"
            else:  # windows
                lib_file = "bson-1.0.dll"
            lib_dir = "bin" if IS_WIN else "lib*"
            lib_dir = glob.glob(os.path.join(install_dir, lib_dir))
            if lib_dir:
                lib_file = os.path.join(lib_dir[0], lib_file)
                if os.path.exists(lib_file):
                    shutil.copy(lib_file, BUILD_DIR)

            # Ensure our Cython extension can dynamically link to libraries
            # - https://blog.krzyzanowskim.com/2018/12/05/rpath-what/
            # - https://nehckl0.medium.com/creating-relocatable-linux-executables-by-setting-rpath-with-origin-45de573a2e98
            if platform == "darwin":
                module.extra_link_args += ["-rpath", "@loader_path"]
            elif platform == "linux":
                module.extra_link_args += ["-Wl,-rpath,$ORIGIN"]

        # Find the linkable library file, and explicitly add it to the linker if on Windows.
        lib_dirs = glob.glob(os.path.join(install_dir, "lib*"))
        if len(lib_dirs) != 1:
            warnings.warn(f"Unable to locate libbson in {install_dir}")
            if IS_WIN:
                raise ValueError(
                    "We require a LIBBSON_INSTALL_DIR with a compiled library on Windows"
                )
        else:
            lib_dir = lib_dirs[0]
            if IS_WIN:
                # Note: we replace any forward slashes with backslashes so the path
                # can be parsed by bash.
                lib_path = os.path.join(lib_dir, "bson-1.0.lib").replace(os.sep, "/")
                if os.path.exists(lib_path):
                    module.extra_link_args = [lib_path]
                    include_dir = os.path.join(install_dir, "include", "libbson-1.0").replace(
                        os.sep, "/"
                    )
                    module.include_dirs.append(include_dir)
                else:
                    raise ValueError(f"Could not find the compiled libbson in {install_dir}")
            pc_path = os.path.join(install_dir, lib_dir, "pkgconfig", "libbson-1.0.pc")

    elif IS_WIN:
        raise ValueError("We require a LIBBSON_INSTALL_DIR with a compiled library on Windows")

    if IS_WIN:
        # We have added the library file without raising an error, so return.
        return

    # Check for the existence of the library.
    lnames = query_pkgconfig(f"pkg-config --libs-only-l {pc_path}")
    if not lnames:
        raise ValueError(f'Could not find "{pc_path}" library')

    # Check against the minimum required version.
    min_version = get_min_libbson_version()
    mod_version = query_pkgconfig(f"pkg-config --modversion {pc_path}")

    if mod_version < min_version:
        raise ValueError(
            f"Cannot use {pc_path} with version {mod_version}, minimum required version is {min_version}"
        )

    # Gather the appropriate flags.
    cflags = query_pkgconfig(f"pkg-config --cflags {pc_path}")

    if cflags:
        orig_cflags = os.environ.get("CFLAGS", "")
        os.environ["CFLAGS"] = cflags + " " + orig_cflags

    ldflags = query_pkgconfig(f"pkg-config --libs {pc_path}")
    if ldflags:
        orig_ldflags = os.environ.get("LDFLAGS", "")
        os.environ["LDFLAGS"] = ldflags + " " + orig_ldflags

    # https://cython.readthedocs.io/en/latest/src/tutorial/external.html#dynamic-linking
    # Strip whitespace to avoid weird linker failures on manylinux images
    libnames = [lname.lstrip("-l").strip() for lname in lnames.split()]
    module.libraries.extend(libnames)


def append_arrow_flags(ext):
    import numpy as np
    import pyarrow as pa

    # From https://arrow.apache.org/docs/python/integration/extending.html#example
    # The Numpy C headers are currently required
    ext.include_dirs.append(np.get_include())
    ext.include_dirs.append(pa.get_include())
    ext.libraries.extend(pa.get_libraries())
    ext.library_dirs.extend(pa.get_library_dirs())

    if os.name != "nt" and CREATE_LIBARROW_SYMLINKS:
        # On Linux and MacOS, we must run pyarrow.create_library_symlinks()
        # as a user with write access to the directory where pyarrow is
        # installed.
        # See https://arrow.apache.org/docs/python/integration/extending.html#building-extensions-against-pypi-wheels.
        pa.create_library_symlinks()

    if os.name == "posix":
        ext.extra_compile_args.append("-std=c++17")

    elif os.name == "nt":
        ext.extra_compile_args.append("/std:c++17")


def get_extension_modules():
    # This change is needed in order to allow setuptools to import the
    # library to obtain metadata information outside of a build environment.
    try:
        from Cython.Build import cythonize
    except ImportError:
        warnings.warn("Cannot compile native C code, because of a missing build dependency")
        return []
    modules = cythonize(["pymongoarrow/*.pyx"])
    for module in modules:
        append_libbson_flags(module)
        append_arrow_flags(module)

    return modules


ext_modules = [] if os.environ.get("NO_EXT") else get_extension_modules()

setup(ext_modules=ext_modules)
