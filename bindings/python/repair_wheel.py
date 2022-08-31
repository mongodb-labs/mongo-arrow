import atexit
import glob
import os
import sys
import tempfile
from subprocess import run

HERE = os.path.abspath(os.path.dirname(__file__))
wheel_dir, wheel_file, delocate_args = sys.argv[1:]
wheel_dir = wheel_dir.replace(os.sep, "/")
wheel_file = wheel_file.replace(os.sep, "/")

# Ensure pyarrow.
if "universal2" in wheel_file:
    # pip selects the most specific platform by default,
    # so we have to tell it to install the universal2 version.
    # See https://github.com/pypa/packaging/issues/381
    macos_ver = os.environ.get("MACOSX_DEPLOYMENT_TARGET", "10.3")
    macos_ver = macos_ver.replace(".", "_")
    wheel_temp_dir = tempfile.TemporaryDirectory()
    atexit.register(wheel_temp_dir.cleanup)
    run(
        [
            sys.executable,
            "-m",
            "pip",
            "install",
            "--platform",
            f"macosx_{macos_ver}_universal2",
            "--upgrade",
            "--target",
            wheel_temp_dir.name,
            "--only-binary=:all:",
            "pyarrow",
        ]
    )
    # Allow the installed pyarrow library to be imported.
    sys.path.insert(0, wheel_temp_dir.name)
else:
    run([sys.executable, "-m", "pip", "install", "pyarrow"])
import pyarrow as pa  # noqa

libbson = os.environ.get("LIBBSON_INSTALL_DIR", os.path.join(HERE, "libbson"))
libbson = os.path.abspath(libbson)
if os.name == "nt":
    libbson_lib = glob.glob(os.path.join(libbson, "bin"))
else:
    libbson_lib = glob.glob(os.path.join(libbson, "lib*"))
extra_paths = pa.get_library_dirs() + libbson_lib
extra_path = os.path.pathsep.join([a.replace(os.sep, "/") for a in extra_paths])


def append_os_variable(name, extra_path):
    if os.environ.get(name):
        os.environ[name] = os.environ[name] + os.pathsep + extra_path
    else:
        os.environ[name] = extra_path
    print(f"{name}: {os.environ[name]}")


if os.name == "nt":
    append_os_variable("PATH", extra_path)
    run([sys.executable, "-m", "pip", "install", "delvewheel"])
    # Do not mangle ucrtbased.dll to avoid:
    # "RuntimeError: Unable to rename the dependencies of vcruntime140d.dll
    # because this DLL has trailing data. If this DLL was created with MinGW,
    # run the strip utility. Otherwise, include ucrtbased.dll in the
    # --no-mangle flag. In addition, if you believe that delvewheel should
    # avoid name-mangling a specific DLL by default, open an issue at
    # https://github.com/adang1345/delvewheel/issues and include this error
    # message."
    run(["delvewheel", "repair", "--no-mangle", "ucrtbased.dll", "-w", wheel_dir, wheel_file])

elif sys.platform == "darwin":
    # FIXME: We should not have to do this.
    site_pkgs = sys.base_prefix
    dylib = glob.glob(f"{sys.base_prefix}/lib/python*/lib-dynload")[0]
    extra_path = f"{dylib}:{extra_path}"
    append_os_variable("DYLD_LIBRARY_PATH", extra_path)
    run([sys.executable, "-m", "pip", "install", "delocate"])
    run(
        [
            "delocate-wheel",
            "--require-archs",
            delocate_args,
            "-w",
            wheel_dir,
            wheel_file,
        ]
    )
else:
    append_os_variable("LD_LIBRARY_PATH", extra_path)
    run([sys.executable, "-m", "pip", "install", "auditwheel"])
    run(["auditwheel", "repair", "-w", wheel_dir, wheel_file])
