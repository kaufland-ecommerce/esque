import datetime
from typing import Any, List

import pytest

from esque.io.serializers.json import JsonSerializer, JsonSerializerConfig

CET = datetime.timezone(datetime.timedelta(seconds=3600), "CET")
EXPECTED_DATA = {
    "str_field": "Übung",
    "int_field": 1337,
    "float_field": 1.337,
    "date_field": datetime.date(2020, 1, 1),
    "datetime_field": datetime.datetime(2020, 1, 1, 2, 3, 4),
    "timestamp_field": datetime.datetime(2020, 1, 1, 2, 3, 4).astimezone(CET),
    "bytes_field": b"\x00\x01\x02",
}

# Although removing them would make the serialized data more compact, we decided to keep the space after ':' and ','
# because we think the json serializer is more used for readability than for actual data serialization
EXPECTED_SERIALIZED_DATA = (
    b'{"str_field": "\\u00dcbung", "int_field": 1337, "float_field": 1.337, "date_field": "2020-01-01", '
    b'"datetime_field": "2020-01-01T02:03:04", "timestamp_field": "2020-01-01T02:03:04+01:00", "bytes_field": "AAEC"}'
)

EXPECTED_DESERIALIZED_DATA = {
    "str_field": "Übung",
    "int_field": 1337,
    "float_field": 1.337,
    "date_field": "2020-01-01",
    "datetime_field": "2020-01-01T02:03:04",
    "timestamp_field": "2020-01-01T02:03:04+01:00",
    "bytes_field": "AAEC",
}


@pytest.fixture
def serializer() -> JsonSerializer:
    return JsonSerializer(JsonSerializerConfig(scheme="json", indent=None))


def test_json_serializer(serializer):
    actual_serialized_data: bytes = serializer.serialize(EXPECTED_DATA)
    assert actual_serialized_data == EXPECTED_SERIALIZED_DATA

    actual_deserialized_data: Any = serializer.deserialize(actual_serialized_data)
    assert actual_deserialized_data == EXPECTED_DESERIALIZED_DATA


def test_json_serializer_many(serializer):
    actual_serialized_data: List[bytes] = list(serializer.serialize_many([EXPECTED_DATA, {"a": "b"}]))
    assert actual_serialized_data == [EXPECTED_SERIALIZED_DATA, b'{"a": "b"}']

    actual_deserialized_data: Any = list(serializer.deserialize_many(actual_serialized_data))
    assert actual_deserialized_data == [EXPECTED_DESERIALIZED_DATA, {"a": "b"}]
