"""Tests for sync_pyarrow_pin.py. Run with `python -m pytest tools/`"""

from sync_pyarrow_pin import main

TEMPLATE = """\
[build-system]
requires = [
    "scikit-build-core>=0.10",
    # Exact pin; must equal the floor in project.dependencies.
    "pyarrow=={pin}",
]
build-backend = "scikit_build_core.build"

[project]
dependencies = [
    # The floor must equal the build-time pin in "build-system.requires" above.
    "pyarrow >={floor},<{ceiling}",
    "pymongo >=4.4,<5",
]
"""


def write_pyproject(tmp_path, pin, floor, ceiling="25.1"):
    path = tmp_path / "pyproject.toml"
    path.write_text(TEMPLATE.format(pin=pin, floor=floor, ceiling=ceiling))
    return path


def test_in_sync_exits_zero_and_leaves_file_untouched(tmp_path):
    path = write_pyproject(tmp_path, pin="25.0.1", floor="25.0.1")
    before = path.read_text()

    assert main(path) == 0
    assert path.read_text() == before


def test_out_of_sync_rewrites_floor_and_preserves_ceiling(tmp_path):
    path = write_pyproject(tmp_path, pin="25.0.1", floor="24.0.0", ceiling="25.1")

    assert main(path) == 1
    result = path.read_text()
    assert '"pyarrow >=25.0.1,<25.1"' in result
    expected = TEMPLATE.format(pin="25.0.1", floor="25.0.1", ceiling="25.1")
    assert result == expected
