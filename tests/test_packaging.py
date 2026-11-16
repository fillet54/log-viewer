"""Every non-Python file under src/cinc must be shipped by a package-data glob."""

from __future__ import annotations

import glob
from pathlib import Path

import pytest

try:
    import tomllib
except ModuleNotFoundError:  # Python 3.10
    try:
        import tomli as tomllib
    except ModuleNotFoundError:
        tomllib = None

ROOT = Path(__file__).resolve().parents[1]
PACKAGE = ROOT / "src" / "cinc"


@pytest.mark.skipif(tomllib is None, reason="needs tomllib or tomli")
def test_every_asset_matches_a_package_data_glob():
    config = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    patterns = config["tool"]["setuptools"]["package-data"]["cinc"]

    shipped = set()
    for pattern in patterns:
        for match in glob.glob(pattern, root_dir=PACKAGE, recursive=True):
            shipped.add(Path(match).as_posix())

    missing = []
    for path in PACKAGE.rglob("*"):
        if not path.is_file() or "__pycache__" in path.parts:
            continue
        if path.suffix in {".py", ".pyc"}:
            continue
        rel = path.relative_to(PACKAGE).as_posix()
        if rel not in shipped:
            missing.append(rel)

    assert not missing, f"not covered by package-data: {sorted(missing)}"
