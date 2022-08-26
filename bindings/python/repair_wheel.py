import glob
import os
import site
import sys
from subprocess import run

HERE = os.path.abspath(os.path.dirname(__file__))
wheel_dir, wheel_file, delocate_args = sys.argv[1:]
wheel_dir = wheel_dir.replace(os.sep, "/")
wheel_file = wheel_file.replace(os.sep, "/")

# Ensure pyarrow
run([sys.executable, "-m", "pip", "install", "pyarrow"])
import pyarrow as pa  # noqa

libbson = os.environ.get("LIBBSON_INSTALL_DIR", os.path.join(HERE, "libbson"))
libbson = os.path.abspath(libbson)
if os.name == "nt":
    libbson_lib = glob.glob(os.path.join(libbson, "bin"))
else:
    libbson_lib = glob.glob(os.path.join(libbson, "lib*"))
extra_path = pa.get_library_dirs() + libbson_lib
extra_path = os.path.pathsep.join([a.replace(os.sep, "/") for a in extra_path])

if os.name == "nt":
    run([sys.executable, "-m", "pip", "install", "delvewheel"])
    os.environ["PATH"] = extra_path + os.path.pathsep + os.environ["PATH"]
    print("PATH:", os.environ["PATH"])
    run(["delvewheel", "repair", "--no-mangle", "ucrtbased.dll", "-w", wheel_dir, wheel_file])

elif sys.platform == "darwin":
    # FIXME: We should not have to do this:
    site_pkgs = site.getsitepackages()[0]
    dylib = os.path.join(os.path.dirname(site_pkgs), "lib-dynload")
    print(os.listdir(dylib))
    extra_path = f"{dylib}:{extra_path}"

    if os.environ.get("DYLD_LIBRARY_PATH"):
        os.environ["DYLD_LIBRARY_PATH"] = os.environ["DYLD_LIBRARY_PATH"] + ":" + extra_path
    else:
        os.environ["DYLD_LIBRARY_PATH"] = extra_path

    print("DYLD_LIBRARY_PATH:", os.environ["DYLD_LIBRARY_PATH"])
    run([sys.executable, "-m", "pip", "install", "delocate"])
    run(
        [
            sys.executable,
            "-m",
            "delocate.cmd.delocate_wheel",
            "--require-archs",
            delocate_args,
            "-w",
            wheel_dir,
            wheel_file,
        ]
    )
else:
    if os.environ.get("LD_LIBRARY_PATH"):
        os.environ["LD_LIBRARY_PATH"] = os.environ["LD_LIBRARY_PATH"] + ":" + extra_path
    else:
        os.environ["LD_LIBRARY_PATH"] = extra_path
    print("LD_LIBRARY_PATH:", os.environ["LD_LIBRARY_PATH"])
    run([sys.executable, "-m", "pip", "install", "auditwheel"])
    run(["auditwheel", "repair", "-w", wheel_dir, wheel_file])
