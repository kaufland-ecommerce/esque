from dataclasses import dataclass
from typing import Dict, List, Optional

from .base import ApiKey, apiKeySerializer, RequestData, ResponseData, errorCodeSerializer
from ..serializers import Schema, int16Serializer, BaseSerializer, NamedTupleSerializer, ArraySerializer, \
    DummySerializer, int32Serializer
from ..constants import ErrorCode


@dataclass
class ApiSupportRange:
    api_key: ApiKey
    min_version: int  # INT16
    max_version: int  # INT16


apiSupportRangeSchema: Schema = [
    ("api_key", apiKeySerializer),
    ("min_version", int16Serializer),
    ("max_version", int16Serializer),
]

apiSupportRangeSerializer: BaseSerializer[ApiSupportRange] = NamedTupleSerializer(
    ApiSupportRange, apiSupportRangeSchema
)


class ApiVersionRequestData(RequestData):
    @staticmethod
    def api_key() -> ApiKey:
        return ApiKey.API_VERSIONS


apiVersionRequestSchemas: Dict[int, Schema] = {0: [], 1: [], 2: []}


apiVersionRequestSerializers: Dict[int, BaseSerializer[ApiVersionRequestData]] = {
    version: NamedTupleSerializer(ApiVersionRequestData, schema) for version, schema in apiVersionRequestSchemas.items()
}


@dataclass
class ApiVersionResponseData(ResponseData):
    error_code: ErrorCode  # INT16
    api_versions: List[ApiSupportRange]
    throttle_time_ms: Optional[int]  # INT32

    @staticmethod
    def api_key() -> ApiKey:
        return ApiKey.API_VERSIONS


apiVersionResponseSchemas: Dict[int, Schema] = {
    0: [
        ("error_code", errorCodeSerializer),
        ("api_versions", ArraySerializer(apiSupportRangeSerializer)),
        ("throttle_time_ms", DummySerializer(None)),
    ],
    1: [
        ("error_code", errorCodeSerializer),
        ("api_versions", ArraySerializer(apiSupportRangeSerializer)),
        ("throttle_time_ms", int32Serializer),
    ],
    2: [
        ("error_code", errorCodeSerializer),
        ("api_versions", ArraySerializer(apiSupportRangeSerializer)),
        ("throttle_time_ms", int32Serializer),
    ],
}

apiVersionResponseSerializers: Dict[int, BaseSerializer[ApiVersionResponseData]] = {
    version: NamedTupleSerializer(ApiVersionResponseData, schema)
    for version, schema in apiVersionResponseSchemas.items()
}