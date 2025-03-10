import base64
import datetime

from pytest_cases import parametrize_with_cases

from esque.io.messages import Data
from esque.io.serializers.proto import ProtoSerializer

CET = datetime.timezone(datetime.timedelta(seconds=3600), "CET")


def proto_cases_only_name_is_set():
    return (
        "CgdlYnJhaGlt",
        {
            "type_string": "ebrahim",
            "type_enum": "ENUM_TYPE_UNSPECIFIED",
            "type_int32": 0,
            "type_int64": "0",
            "type_float": 0,
        },
    )


def proto_cases_when_optional_name_is_set():
    return (
        "EgtpbSBvcHRpb25hbA==",
        {
            "optional_string": "im optional",
            "type_string": "",
            "type_enum": "ENUM_TYPE_UNSPECIFIED",
            "type_int32": 0,
            "type_int64": "0",
            "type_float": 0.0,
        },
    )


def proto_cases_when_type_float_is_set():
    return (
        "EgtpbSBvcHRpb25hbD0ZBJ4/",
        {
            "optional_string": "im optional",
            "type_string": "",
            "type_enum": "ENUM_TYPE_UNSPECIFIED",
            "type_int32": 0,
            "type_int64": "0",
            "type_float": 1.2345,
        },
    )


@parametrize_with_cases(argnames=("b64", "expected"), prefix="proto_cases", cases=".")
def test_proto_deserializer(proto_serializer, b64, expected):
    actual_result = proto_serializer.deserialize(base64.b64decode(b64))
    assert actual_result.payload == expected


@parametrize_with_cases(argnames=("b64", "input"), prefix="proto_cases", cases=".")
def test_proto_serializer(proto_serializer, b64, input: dict):
    actual_result = proto_serializer.serialize(Data(input, ProtoSerializer.dict_data_type))
    assert actual_result == base64.b64decode(b64)
