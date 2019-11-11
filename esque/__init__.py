import configparser
from pathlib import Path

parser = configparser.ConfigParser()

pyproject = Path(__file__).parent.parent / "pyproject.toml"
assert pyproject.exists(), "pyproject.toml doesn't exist"
parser.read(pyproject)
__version__ = parser["tool.poetry"]["version"]
