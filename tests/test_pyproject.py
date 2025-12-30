import pathlib
import tomllib


def test_pyproject_name():
    p = pathlib.Path("pyproject.toml")
    data = tomllib.loads(p.read_text())
    assert data.get("project", {}).get("name") == "opteryx-catalog"
