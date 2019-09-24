# FIXME autogenerated module, check for errors!
from dataclasses import dataclass
from typing import Dict, Tuple, List, Optional

from esque.protocol.api.base import *
from esque.protocol.serializers import *


@dataclass
class Partitions:
    # Topic partition id
    partition: "int"  # INT32

    # The current leader epoch, if provided, is used to fence consumers/replicas with old metadata. If the
    # epoch provided by the client is larger than the current epoch known to the broker, then the
    # UNKNOWN_LEADER_EPOCH error code will be returned. If the provided epoch is smaller, then the
    # FENCED_LEADER_EPOCH error code will be returned.
    current_leader_epoch: "int"  # INT32

    # The epoch to lookup an offset for.
    leader_epoch: "int"  # INT32


@dataclass
class Topics:
    # Name of topic
    topic: "str"  # STRING

    # An array of partitions to get epochs for
    partitions: List["Partitions"]


@dataclass
class OffsetForLeaderEpochRequestData(RequestData):
    # Broker id of the follower. For normal consumers, use -1.
    replica_id: "int"  # INT32

    # An array of topics to get epochs for
    topics: List["Topics"]

    @staticmethod
    def api_key() -> int:
        return ApiKey.OFFSET_FOR_LEADER_EPOCH  # == 23


@dataclass
class Partitions:
    # Response error code
    error_code: "int"  # INT16

    # Topic partition id
    partition: "int"  # INT32

    # The leader epoch
    leader_epoch: "int"  # INT32

    # The end offset
    end_offset: "int"  # INT64


@dataclass
class Topics:
    # Name of topic
    topic: "str"  # STRING

    # An array of offsets by partition
    partitions: List["Partitions"]


@dataclass
class OffsetForLeaderEpochResponseData(ResponseData):
    # Duration in milliseconds for which the request was throttled due to quota violation (Zero if the
    # request did not violate any quota)
    throttle_time_ms: "int"  # INT32

    # An array of topics for which we have leader offsets for some requested partition leader epoch
    topics: List["Topics"]

    @staticmethod
    def api_key() -> int:
        return ApiKey.OFFSET_FOR_LEADER_EPOCH  # == 23


partitionsSchemas: Dict[int, Schema] = {
    0: [
        ("partition", int32Serializer),
        ("leader_epoch", int32Serializer),
        ("current_leader_epoch", DummySerializer(int())),
    ],
    1: [
        ("partition", int32Serializer),
        ("leader_epoch", int32Serializer),
        ("current_leader_epoch", DummySerializer(int())),
    ],
    2: [("partition", int32Serializer), ("current_leader_epoch", int32Serializer), ("leader_epoch", int32Serializer)],
    3: [("partition", int32Serializer), ("current_leader_epoch", int32Serializer), ("leader_epoch", int32Serializer)],
}


partitionsSerializers: Dict[int, BaseSerializer[Partitions]] = {
    version: NamedTupleSerializer(Partitions, schema) for version, schema in partitionsSchemas.items()
}


topicsSchemas: Dict[int, Schema] = {
    0: [("topic", stringSerializer), ("partitions", ArraySerializer(partitionsSerializers[0]))],
    1: [("topic", stringSerializer), ("partitions", ArraySerializer(partitionsSerializers[1]))],
    2: [("topic", stringSerializer), ("partitions", ArraySerializer(partitionsSerializers[2]))],
    3: [("topic", stringSerializer), ("partitions", ArraySerializer(partitionsSerializers[3]))],
}


topicsSerializers: Dict[int, BaseSerializer[Topics]] = {
    version: NamedTupleSerializer(Topics, schema) for version, schema in topicsSchemas.items()
}


offsetForLeaderEpochRequestDataSchemas: Dict[int, Schema] = {
    0: [("topics", ArraySerializer(topicsSerializers[0])), ("replica_id", DummySerializer(int()))],
    1: [("topics", ArraySerializer(topicsSerializers[1])), ("replica_id", DummySerializer(int()))],
    2: [("topics", ArraySerializer(topicsSerializers[2])), ("replica_id", DummySerializer(int()))],
    3: [("replica_id", int32Serializer), ("topics", ArraySerializer(topicsSerializers[3]))],
}


offsetForLeaderEpochRequestDataSerializers: Dict[int, BaseSerializer[OffsetForLeaderEpochRequestData]] = {
    version: NamedTupleSerializer(OffsetForLeaderEpochRequestData, schema)
    for version, schema in offsetForLeaderEpochRequestDataSchemas.items()
}


partitionsSchemas: Dict[int, Schema] = {
    0: [
        ("error_code", int16Serializer),
        ("partition", int32Serializer),
        ("end_offset", int64Serializer),
        ("leader_epoch", DummySerializer(int())),
    ],
    1: [
        ("error_code", int16Serializer),
        ("partition", int32Serializer),
        ("leader_epoch", int32Serializer),
        ("end_offset", int64Serializer),
    ],
    2: [
        ("error_code", int16Serializer),
        ("partition", int32Serializer),
        ("leader_epoch", int32Serializer),
        ("end_offset", int64Serializer),
    ],
    3: [
        ("error_code", int16Serializer),
        ("partition", int32Serializer),
        ("leader_epoch", int32Serializer),
        ("end_offset", int64Serializer),
    ],
}


partitionsSerializers: Dict[int, BaseSerializer[Partitions]] = {
    version: NamedTupleSerializer(Partitions, schema) for version, schema in partitionsSchemas.items()
}


topicsSchemas: Dict[int, Schema] = {
    0: [("topic", stringSerializer), ("partitions", ArraySerializer(partitionsSerializers[0]))],
    1: [("topic", stringSerializer), ("partitions", ArraySerializer(partitionsSerializers[1]))],
    2: [("topic", stringSerializer), ("partitions", ArraySerializer(partitionsSerializers[2]))],
    3: [("topic", stringSerializer), ("partitions", ArraySerializer(partitionsSerializers[3]))],
}


topicsSerializers: Dict[int, BaseSerializer[Topics]] = {
    version: NamedTupleSerializer(Topics, schema) for version, schema in topicsSchemas.items()
}


offsetForLeaderEpochResponseDataSchemas: Dict[int, Schema] = {
    0: [("topics", ArraySerializer(topicsSerializers[0])), ("throttle_time_ms", DummySerializer(int()))],
    1: [("topics", ArraySerializer(topicsSerializers[1])), ("throttle_time_ms", DummySerializer(int()))],
    2: [("throttle_time_ms", int32Serializer), ("topics", ArraySerializer(topicsSerializers[2]))],
    3: [("throttle_time_ms", int32Serializer), ("topics", ArraySerializer(topicsSerializers[3]))],
}


offsetForLeaderEpochResponseDataSerializers: Dict[int, BaseSerializer[OffsetForLeaderEpochResponseData]] = {
    version: NamedTupleSerializer(OffsetForLeaderEpochResponseData, schema)
    for version, schema in offsetForLeaderEpochResponseDataSchemas.items()
}