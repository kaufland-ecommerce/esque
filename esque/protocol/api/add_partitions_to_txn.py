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
class Topics:
    # Name of topic
    topic: "str"  # STRING

    partitions: List["int"]  # INT32


@dataclass
class AddPartitionsToTxnRequestData(RequestData):
    # The transactional id corresponding to the transaction.
    transactional_id: "str"  # STRING

    # Current producer id in use by the transactional id.
    producer_id: "int"  # INT64

    # Current epoch associated with the producer id.
    producer_epoch: "int"  # INT16

    # The partitions to add to the transaction.
    topics: List["Topics"]

    @staticmethod
    def api_key() -> int:
        return ApiKey.ADD_PARTITIONS_TO_TXN  # == 24


@dataclass
class PartitionErrors:
    # Topic partition id
    partition: "int"  # INT32

    # Response error code
    error_code: "int"  # INT16


@dataclass
class Errors:
    # Name of topic
    topic: "str"  # STRING

    partition_errors: List["PartitionErrors"]


@dataclass
class AddPartitionsToTxnResponseData(ResponseData):
    # Duration in milliseconds for which the request was throttled due to quota violation (Zero if the
    # request did not violate any quota)
    throttle_time_ms: "int"  # INT32

    errors: List["Errors"]

    @staticmethod
    def api_key() -> int:
        return ApiKey.ADD_PARTITIONS_TO_TXN  # == 24


topicsSchemas: Dict[int, Schema] = {
    0: [("topic", stringSerializer), ("partitions", ArraySerializer(int32Serializer))],
    1: [("topic", stringSerializer), ("partitions", ArraySerializer(int32Serializer))],
}


topicsSerializers: Dict[int, BaseSerializer[Topics]] = {
    version: NamedTupleSerializer(Topics, schema) for version, schema in topicsSchemas.items()
}


addPartitionsToTxnRequestDataSchemas: Dict[int, Schema] = {
    0: [
        ("transactional_id", stringSerializer),
        ("producer_id", int64Serializer),
        ("producer_epoch", int16Serializer),
        ("topics", ArraySerializer(topicsSerializers[0])),
    ],
    1: [
        ("transactional_id", stringSerializer),
        ("producer_id", int64Serializer),
        ("producer_epoch", int16Serializer),
        ("topics", ArraySerializer(topicsSerializers[1])),
    ],
}


addPartitionsToTxnRequestDataSerializers: Dict[int, BaseSerializer[AddPartitionsToTxnRequestData]] = {
    version: NamedTupleSerializer(AddPartitionsToTxnRequestData, schema)
    for version, schema in addPartitionsToTxnRequestDataSchemas.items()
}


partitionErrorsSchemas: Dict[int, Schema] = {
    0: [("partition", int32Serializer), ("error_code", int16Serializer)],
    1: [("partition", int32Serializer), ("error_code", int16Serializer)],
}


partitionErrorsSerializers: Dict[int, BaseSerializer[PartitionErrors]] = {
    version: NamedTupleSerializer(PartitionErrors, schema) for version, schema in partitionErrorsSchemas.items()
}


errorsSchemas: Dict[int, Schema] = {
    0: [("topic", stringSerializer), ("partition_errors", ArraySerializer(partitionErrorsSerializers[0]))],
    1: [("topic", stringSerializer), ("partition_errors", ArraySerializer(partitionErrorsSerializers[1]))],
}


errorsSerializers: Dict[int, BaseSerializer[Errors]] = {
    version: NamedTupleSerializer(Errors, schema) for version, schema in errorsSchemas.items()
}


addPartitionsToTxnResponseDataSchemas: Dict[int, Schema] = {
    0: [("throttle_time_ms", int32Serializer), ("errors", ArraySerializer(errorsSerializers[0]))],
    1: [("throttle_time_ms", int32Serializer), ("errors", ArraySerializer(errorsSerializers[1]))],
}


addPartitionsToTxnResponseDataSerializers: Dict[int, BaseSerializer[AddPartitionsToTxnResponseData]] = {
    version: NamedTupleSerializer(AddPartitionsToTxnResponseData, schema)
    for version, schema in addPartitionsToTxnResponseDataSchemas.items()
}
