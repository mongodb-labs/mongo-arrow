"""Sync the pyarrow floor in project.dependencies with the build-time pin.

The exact pin in build-system.requires ("pyarrow==X.Y.Z") must equal the
floor of the runtime dependency ("pyarrow >=X.Y.Z,<..."), because Cython
bakes the build-time layout of pyarrow's cdef classes into the extension.
This script rewrites the floor to match the pin, preserving the ceiling,
and exits 1 when it had to change something.
"""

import re
import sys
from pathlib import Path

PIN_RE = re.compile(r'"pyarrow==(\d+(?:\.\d+)*)"')
FLOOR_RE = re.compile(r'("pyarrow >=)(\d+(?:\.\d+)*)(,<[^"]*")')

REPO_PYPROJECT = Path(__file__).resolve().parents[1] / "pyproject.toml"


def main(path: Path) -> int:
    text = path.read_text()

    pin_match = PIN_RE.search(text)
    if pin_match is None:
        print(f"{path}: no pyarrow pin found in build-system.requires")
        return 1
    pin = pin_match.group(1)

    floor_match = FLOOR_RE.search(text)
    if floor_match is None:
        print(f"{path}: no pyarrow floor found in project.dependencies")
        return 1
    floor = floor_match.group(2)

    if floor == pin:
        return 0

    path.write_text(text[: floor_match.start(2)] + pin + text[floor_match.end(2) :])
    print(f"{path}: synced pyarrow floor {floor} -> {pin} to match build pin")
    return 1


if __name__ == "__main__":
    sys.exit(main(REPO_PYPROJECT))
