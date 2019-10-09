import json
from typing import Callable

import pytest

from esque.cli.output import format_output
from tests.conftest import check_and_load_yaml

FORMATS_AND_LOADERS = [("yaml", check_and_load_yaml), ("json", json.loads)]

TUPLE_BYTES_DICT = {
    "list": [1, 2, "3"],
    "string": "string",
    "int": 4,
    "tuple": ("a", "b"),
    "bytes": str(4).encode(),
}


def test_format_output_default():
    pretty_output = format_output(TUPLE_BYTES_DICT, None)
    for key in TUPLE_BYTES_DICT:
        assert key in pretty_output


@pytest.mark.parametrize("output_format,loader", FORMATS_AND_LOADERS, ids=["yaml", "json"])
def test_format_output(output_format: str, loader: Callable):
    dumped_dict = format_output(TUPLE_BYTES_DICT, output_format)
    loaded_dict = loader(dumped_dict)
    check_loaded_dict(TUPLE_BYTES_DICT, loaded_dict)
    assert loaded_dict["tuple"] == ["a", "b"]
    assert loaded_dict["bytes"] == "4"


def check_loaded_dict(original_dict: dict, loaded_dict: dict):
    assert len(loaded_dict) == len(original_dict)
    for key in original_dict.keys():
        assert key in loaded_dict
        original_value = original_dict[key]
        if type(original_value) == tuple:
            assert list(original_value) == loaded_dict[key]
        elif type(original_value) == bytes:
            assert original_value.decode("UTF-8") == loaded_dict[key]
        else:
            assert original_value == loaded_dict[key]
