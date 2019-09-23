# FIXME autogenerated module, check for errors!
from dataclasses import dataclass
from typing import Dict, Tuple, List, Optional

from esque.protocol.api.base import *
from esque.protocol.serializers import *


@dataclass
class TopicPartitions:
    # The name of a topic.
    topic: "str"  # STRING

    # The partitions of this topic whose preferred leader should be elected
    partition_id: List["int"]  # INT32


@dataclass
class ElectPreferredLeadersRequestData(RequestData):
    # The topic partitions to elect the preferred leader of.
    topic_partitions: List["TopicPartitions"]

    # The time in ms to wait for the election to complete.
    timeout_ms: "int"  # INT32

    @staticmethod
    def api_key() -> int:
        return ApiKey.ELECT_PREFERRED_LEADERS  # == 43


@dataclass
class PartitionResult:
    # The partition id
    partition_id: "int"  # INT32

    # The result error, or zero if there was no error.
    error_code: "int"  # INT16

    # The result message, or null if there was no error.
    error_message: "Optional[str]"  # NULLABLE_STRING


@dataclass
class ReplicaElectionResults:
    # The topic name
    topic: "str"  # STRING

    # The results for each partition
    partition_result: List["PartitionResult"]


@dataclass
class ElectPreferredLeadersResponseData(ResponseData):
    # The duration in milliseconds for which the request was throttled due to a quota violation, or zero
    # if the request did not violate any quota.
    throttle_time_ms: "int"  # INT32

    # The election results, or an empty array if the requester did not have permission and the request
    # asks for all partitions.
    replica_election_results: List["ReplicaElectionResults"]

    @staticmethod
    def api_key() -> int:
        return ApiKey.ELECT_PREFERRED_LEADERS  # == 43


topicPartitionsSchemas: Dict[int, Schema] = {
    0: [("topic", stringSerializer), ("partition_id", ArraySerializer(int32Serializer))]
}


topicPartitionsSerializers: Dict[int, BaseSerializer[TopicPartitions]] = {
    version: NamedTupleSerializer(TopicPartitions, schema) for version, schema in topicPartitionsSchemas.items()
}


electPreferredLeadersRequestDataSchemas: Dict[int, Schema] = {
    0: [("topic_partitions", ArraySerializer(topicPartitionsSerializers[0])), ("timeout_ms", int32Serializer)]
}


electPreferredLeadersRequestDataSerializers: Dict[int, BaseSerializer[ElectPreferredLeadersRequestData]] = {
    version: NamedTupleSerializer(ElectPreferredLeadersRequestData, schema)
    for version, schema in electPreferredLeadersRequestDataSchemas.items()
}


partitionResultSchemas: Dict[int, Schema] = {
    0: [
        ("partition_id", int32Serializer),
        ("error_code", int16Serializer),
        ("error_message", nullableStringSerializer),
    ]
}


partitionResultSerializers: Dict[int, BaseSerializer[PartitionResult]] = {
    version: NamedTupleSerializer(PartitionResult, schema) for version, schema in partitionResultSchemas.items()
}


replicaElectionResultsSchemas: Dict[int, Schema] = {
    0: [("topic", stringSerializer), ("partition_result", ArraySerializer(partitionResultSerializers[0]))]
}


replicaElectionResultsSerializers: Dict[int, BaseSerializer[ReplicaElectionResults]] = {
    version: NamedTupleSerializer(ReplicaElectionResults, schema)
    for version, schema in replicaElectionResultsSchemas.items()
}


electPreferredLeadersResponseDataSchemas: Dict[int, Schema] = {
    0: [
        ("throttle_time_ms", int32Serializer),
        ("replica_election_results", ArraySerializer(replicaElectionResultsSerializers[0])),
    ]
}


electPreferredLeadersResponseDataSerializers: Dict[int, BaseSerializer[ElectPreferredLeadersResponseData]] = {
    version: NamedTupleSerializer(ElectPreferredLeadersResponseData, schema)
    for version, schema in electPreferredLeadersResponseDataSchemas.items()
}
