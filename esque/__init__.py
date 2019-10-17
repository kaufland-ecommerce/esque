import configparser

parser = configparser.ConfigParser()
parser.read("pyproject.toml")
__version__ = parser["tool.poetry"]["version"]
