from pathlib import Path

import toml

pyproject = Path(__file__).parent.parent / "pyproject.toml"
assert pyproject.exists(), "pyproject.toml doesn't exist"
data = toml.loads(pyproject.read_text())
__version__ = data["tool"]["poetry"]["version"]
