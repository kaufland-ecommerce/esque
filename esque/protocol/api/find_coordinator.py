# FIXME autogenerated module, check for errors!
from dataclasses import dataclass
from typing import Dict, Tuple, List, Optional

from esque.protocol.api.base import *
from esque.protocol.serializers import *


@dataclass
class FindCoordinatorRequestData(RequestData):
    # The coordinator key.
    key: "str"  # STRING

    # The coordinator key type.  (Group, transaction, etc.)
    key_type: "int"  # INT8

    @staticmethod
    def api_key() -> int:
        return ApiKey.FIND_COORDINATOR  # == 10


@dataclass
class FindCoordinatorResponseData(ResponseData):
    # The duration in milliseconds for which the request was throttled due to a quota violation, or zero
    # if the request did not violate any quota.
    throttle_time_ms: "int"  # INT32

    # The error code, or 0 if there was no error.
    error_code: "int"  # INT16

    # The error message, or null if there was no error.
    error_message: "Optional[str]"  # NULLABLE_STRING

    # The node id.
    node_id: "int"  # INT32

    # The host name.
    host: "str"  # STRING

    # The port.
    port: "int"  # INT32

    @staticmethod
    def api_key() -> int:
        return ApiKey.FIND_COORDINATOR  # == 10


findCoordinatorRequestDataSchemas: Dict[int, Schema] = {
    0: [("key", stringSerializer), ("key_type", DummySerializer(int()))],
    1: [("key", stringSerializer), ("key_type", int8Serializer)],
    2: [("key", stringSerializer), ("key_type", int8Serializer)],
}


findCoordinatorRequestDataSerializers: Dict[int, BaseSerializer[FindCoordinatorRequestData]] = {
    version: NamedTupleSerializer(FindCoordinatorRequestData, schema)
    for version, schema in findCoordinatorRequestDataSchemas.items()
}


findCoordinatorResponseDataSchemas: Dict[int, Schema] = {
    0: [
        ("error_code", int16Serializer),
        ("node_id", int32Serializer),
        ("host", stringSerializer),
        ("port", int32Serializer),
        ("throttle_time_ms", DummySerializer(int())),
        ("error_message", DummySerializer(None)),
    ],
    1: [
        ("throttle_time_ms", int32Serializer),
        ("error_code", int16Serializer),
        ("error_message", nullableStringSerializer),
        ("node_id", int32Serializer),
        ("host", stringSerializer),
        ("port", int32Serializer),
    ],
    2: [
        ("throttle_time_ms", int32Serializer),
        ("error_code", int16Serializer),
        ("error_message", nullableStringSerializer),
        ("node_id", int32Serializer),
        ("host", stringSerializer),
        ("port", int32Serializer),
    ],
}


findCoordinatorResponseDataSerializers: Dict[int, BaseSerializer[FindCoordinatorResponseData]] = {
    version: NamedTupleSerializer(FindCoordinatorResponseData, schema)
    for version, schema in findCoordinatorResponseDataSchemas.items()
}
