import pytest

from esque.cli.output import format_output


def test_format_output():
    format_output({"a": "a"}, "yaml")
