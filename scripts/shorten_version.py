#!/usr/bin/env python
import pathlib
import re

PROJECT_ROOT = pathlib.Path(__file__).parent.parent

PYPROJECT_TOML = PROJECT_ROOT / "pyproject.toml"


def main():
    data = PYPROJECT_TOML.read_text()
    data = shorten_version(data)
    PYPROJECT_TOML.write_text(data)


def shorten_version(version: str) -> str:
    return re.sub(r'(version = "\d+\.\d+\.\d+)[^\w]*(\w)\w*[^\w]*(\d+")', r"\1\2\3", version)


if __name__ == "__main__":
    main()
