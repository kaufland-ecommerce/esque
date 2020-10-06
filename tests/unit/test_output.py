import json
from datetime import date, datetime
from typing import Any, Callable, Dict

from esque.cli.output import format_output, pretty_duration
from esque.messages.avromessage import StdOutAvroWriter
from tests.conftest import parameterized_output_formats

TUPLE_BYTES_DICT = {"list": [1, 2, "3"], "string": "string", "int": 4, "tuple": ("a", "b"), "bytes": str(4).encode()}


def test_format_output_default():
    pretty_output = format_output(TUPLE_BYTES_DICT, None)
    for key in TUPLE_BYTES_DICT:
        assert key in pretty_output


@parameterized_output_formats
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


def test_duration_unlimited():
    assert pretty_duration(92233720368547750000000000000) == "unlimited"


def test_duration_valid():
    assert pretty_duration(9223372036854775) == "292471 years 10 weeks 6 days 4 hours 54 seconds"


def test_converting_unserializable_values_to_strings():
    message: Dict[str, Any] = {
        "simple": "some string",
        "date": date(2020, 10, 5),
        "datetime": datetime(2020, 10, 5, 11, 15, 20),
        "binary": b"Some other string",
    }
    reloaded_json = json.loads(json.dumps(message, default=StdOutAvroWriter.deserializer))
    assert reloaded_json == {
        "simple": "some string",
        "date": "2020-10-05",
        "datetime": "2020-10-05T11:15:20",
        "binary": "U29tZSBvdGhlciBzdHJpbmc=",
    }
