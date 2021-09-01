import datetime
from typing import Any, List

import pytest

from esque.io.messages import Data
from esque.io.serializers.json import JsonSerializer, JsonSerializerConfig

CET = datetime.timezone(datetime.timedelta(seconds=3600), "CET")
ORIGINAL_TEST_DATA = [
    Data(
        {
            "str_field": "Übung",
            "int_field": 1337,
            "float_field": 1.337,
            "date_field": datetime.date(2020, 1, 1),
            "datetime_field": datetime.datetime(2020, 1, 1, 2, 3, 4),
            "timestamp_field": datetime.datetime(2020, 1, 1, 2, 3, 4).astimezone(CET),
            "bytes_field": b"\x00\x01\x02",
        },
        JsonSerializer.unknown_data_type,
    ),
    Data({"a": "b"}, JsonSerializer.unknown_data_type),
]

# Although removing them would make the serialized data more compact, we decided to keep the space after ':' and ','
# because we think the json serializer is more used for readability than for actual data serialization
EXPECTED_SERIALIZED_DATA = [
    (
        b'{"str_field": "\\u00dcbung", "int_field": 1337, "float_field": 1.337, "date_field": "2020-01-01", '
        b'"datetime_field": "2020-01-01T02:03:04", "timestamp_field": "2020-01-01T02:03:04+01:00", "bytes_field": "AAEC"}'
    ),
    b'{"a": "b"}',
]

# TODO: after we introduced proper schema handling for the json serializer this would look the same as
#   Expected data above
EXPECTED_DESERIALIZED_DATA = [
    Data(
        {
            "str_field": "Übung",
            "int_field": 1337,
            "float_field": 1.337,
            "date_field": "2020-01-01",
            "datetime_field": "2020-01-01T02:03:04",
            "timestamp_field": "2020-01-01T02:03:04+01:00",
            "bytes_field": "AAEC",
        },
        JsonSerializer.unknown_data_type,
    ),
    Data({"a": "b"}, JsonSerializer.unknown_data_type),
]


@pytest.fixture
def serializer() -> JsonSerializer:
    return JsonSerializer(JsonSerializerConfig(scheme="json", indent=None))


def test_json_serializer(serializer):
    actual_serialized_data: bytes = serializer.serialize(ORIGINAL_TEST_DATA[0])
    assert actual_serialized_data == EXPECTED_SERIALIZED_DATA[0]
    actual_serialized_data: bytes = serializer.serialize(EXPECTED_DESERIALIZED_DATA[0])
    assert actual_serialized_data == EXPECTED_SERIALIZED_DATA[0]

    actual_deserialized_data: Any = serializer.deserialize(EXPECTED_SERIALIZED_DATA[0])
    assert actual_deserialized_data == EXPECTED_DESERIALIZED_DATA[0]


def test_json_serializer_many(serializer):
    actual_serialized_data: List[bytes] = list(serializer.serialize_many(ORIGINAL_TEST_DATA))
    assert actual_serialized_data == EXPECTED_SERIALIZED_DATA

    actual_deserialized_data: Any = list(serializer.deserialize_many(EXPECTED_SERIALIZED_DATA))
    assert actual_deserialized_data == EXPECTED_DESERIALIZED_DATA
