# FIXME autogenerated module, check for errors!
from typing import Dict

from dataclasses import dataclass

from esque.protocol.api.base import ApiKey, RequestData, ResponseData
from esque.protocol.serializers import (
    BaseSerializer,
    NamedTupleSerializer,
    Schema,
    int16Serializer,
    int32Serializer,
    int64Serializer,
    nullableStringSerializer,
)


@dataclass
class InitProducerIdRequestData(RequestData):
    # The transactional id, or null if the producer is not transactional.
    transactional_id: "Optional[str]"  # NULLABLE_STRING

    # The time in ms to wait for before aborting idle transactions sent by this producer. This is only
    # relevant if a TransactionalId has been defined.
    transaction_timeout_ms: "int"  # INT32

    @staticmethod
    def api_key() -> int:
        return ApiKey.INIT_PRODUCER_ID  # == 22


@dataclass
class InitProducerIdResponseData(ResponseData):
    # The duration in milliseconds for which the request was throttled due to a quota violation, or zero
    # if the request did not violate any quota.
    throttle_time_ms: "int"  # INT32

    # The error code, or 0 if there was no error.
    error_code: "int"  # INT16

    # The current producer id.
    producer_id: "int"  # INT64

    # The current epoch associated with the producer id.
    producer_epoch: "int"  # INT16

    @staticmethod
    def api_key() -> int:
        return ApiKey.INIT_PRODUCER_ID  # == 22


initProducerIdRequestDataSchemas: Dict[int, Schema] = {
    0: [("transactional_id", nullableStringSerializer), ("transaction_timeout_ms", int32Serializer)],
    1: [("transactional_id", nullableStringSerializer), ("transaction_timeout_ms", int32Serializer)],
}


initProducerIdRequestDataSerializers: Dict[int, BaseSerializer[InitProducerIdRequestData]] = {
    version: NamedTupleSerializer(InitProducerIdRequestData, schema)
    for version, schema in initProducerIdRequestDataSchemas.items()
}


initProducerIdResponseDataSchemas: Dict[int, Schema] = {
    0: [
        ("throttle_time_ms", int32Serializer),
        ("error_code", int16Serializer),
        ("producer_id", int64Serializer),
        ("producer_epoch", int16Serializer),
    ],
    1: [
        ("throttle_time_ms", int32Serializer),
        ("error_code", int16Serializer),
        ("producer_id", int64Serializer),
        ("producer_epoch", int16Serializer),
    ],
}


initProducerIdResponseDataSerializers: Dict[int, BaseSerializer[InitProducerIdResponseData]] = {
    version: NamedTupleSerializer(InitProducerIdResponseData, schema)
    for version, schema in initProducerIdResponseDataSchemas.items()
}
