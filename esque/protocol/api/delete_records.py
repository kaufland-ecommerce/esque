# FIXME autogenerated module, check for errors!
from typing import Dict, List

from dataclasses import dataclass

from esque.protocol.api.base import ApiKey, RequestData, ResponseData
from esque.protocol.serializers import (
    ArraySerializer,
    BaseSerializer,
    NamedTupleSerializer,
    Schema,
    int16Serializer,
    int32Serializer,
    int64Serializer,
    stringSerializer,
)


@dataclass
class Partitions:
    # Topic partition id
    partition: "int"  # INT32

    # The offset before which the messages will be deleted. -1 means high-watermark for the partition.
    offset: "int"  # INT64


@dataclass
class Topics:
    # Name of topic
    topic: "str"  # STRING

    partitions: List["Partitions"]


@dataclass
class DeleteRecordsRequestData(RequestData):
    topics: List["Topics"]

    # The maximum time to await a response in ms.
    timeout: "int"  # INT32

    @staticmethod
    def api_key() -> int:
        return ApiKey.DELETE_RECORDS  # == 21


@dataclass
class Partitions:
    # Topic partition id
    partition: "int"  # INT32

    # Smallest available offset of all live replicas
    low_watermark: "int"  # INT64

    # Response error code
    error_code: "int"  # INT16


@dataclass
class Topics:
    # Name of topic
    topic: "str"  # STRING

    partitions: List["Partitions"]


@dataclass
class DeleteRecordsResponseData(ResponseData):
    # Duration in milliseconds for which the request was throttled due to quota violation (Zero if the
    # request did not violate any quota)
    throttle_time_ms: "int"  # INT32

    topics: List["Topics"]

    @staticmethod
    def api_key() -> int:
        return ApiKey.DELETE_RECORDS  # == 21


partitionsSchemas: Dict[int, Schema] = {
    0: [("partition", int32Serializer), ("offset", int64Serializer)],
    1: [("partition", int32Serializer), ("offset", int64Serializer)],
}


partitionsSerializers: Dict[int, BaseSerializer[Partitions]] = {
    version: NamedTupleSerializer(Partitions, schema) for version, schema in partitionsSchemas.items()
}


topicsSchemas: Dict[int, Schema] = {
    0: [("topic", stringSerializer), ("partitions", ArraySerializer(partitionsSerializers[0]))],
    1: [("topic", stringSerializer), ("partitions", ArraySerializer(partitionsSerializers[1]))],
}


topicsSerializers: Dict[int, BaseSerializer[Topics]] = {
    version: NamedTupleSerializer(Topics, schema) for version, schema in topicsSchemas.items()
}


deleteRecordsRequestDataSchemas: Dict[int, Schema] = {
    0: [("topics", ArraySerializer(topicsSerializers[0])), ("timeout", int32Serializer)],
    1: [("topics", ArraySerializer(topicsSerializers[1])), ("timeout", int32Serializer)],
}


deleteRecordsRequestDataSerializers: Dict[int, BaseSerializer[DeleteRecordsRequestData]] = {
    version: NamedTupleSerializer(DeleteRecordsRequestData, schema)
    for version, schema in deleteRecordsRequestDataSchemas.items()
}


partitionsSchemas: Dict[int, Schema] = {
    0: [("partition", int32Serializer), ("low_watermark", int64Serializer), ("error_code", int16Serializer)],
    1: [("partition", int32Serializer), ("low_watermark", int64Serializer), ("error_code", int16Serializer)],
}


partitionsSerializers: Dict[int, BaseSerializer[Partitions]] = {
    version: NamedTupleSerializer(Partitions, schema) for version, schema in partitionsSchemas.items()
}


topicsSchemas: Dict[int, Schema] = {
    0: [("topic", stringSerializer), ("partitions", ArraySerializer(partitionsSerializers[0]))],
    1: [("topic", stringSerializer), ("partitions", ArraySerializer(partitionsSerializers[1]))],
}


topicsSerializers: Dict[int, BaseSerializer[Topics]] = {
    version: NamedTupleSerializer(Topics, schema) for version, schema in topicsSchemas.items()
}


deleteRecordsResponseDataSchemas: Dict[int, Schema] = {
    0: [("throttle_time_ms", int32Serializer), ("topics", ArraySerializer(topicsSerializers[0]))],
    1: [("throttle_time_ms", int32Serializer), ("topics", ArraySerializer(topicsSerializers[1]))],
}


deleteRecordsResponseDataSerializers: Dict[int, BaseSerializer[DeleteRecordsResponseData]] = {
    version: NamedTupleSerializer(DeleteRecordsResponseData, schema)
    for version, schema in deleteRecordsResponseDataSchemas.items()
}
