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

    # Message offset.
    fetch_offset: "int"  # INT64

    # Earliest available offset of the follower replica. The field is only used when request is sent by
    # follower.
    log_start_offset: "int"  # INT64

    # Maximum bytes to fetch.
    partition_max_bytes: "int"  # INT32


@dataclass
class Topics:
    # Name of topic
    topic: "str"  # STRING

    # Partitions to remove from the fetch session.
    partitions: List["Partitions"]


@dataclass
class ForgottenTopicsData:
    # Name of topic
    topic: "str"  # STRING

    # Partitions to remove from the fetch session.
    partitions: List["int"]  # INT32


@dataclass
class FetchRequestData(RequestData):
    # Broker id of the follower. For normal consumers, use -1.
    replica_id: "int"  # INT32

    # Maximum time in ms to wait for the response.
    max_wait_time: "int"  # INT32

    # Minimum bytes to accumulate in the response.
    min_bytes: "int"  # INT32

    # Maximum bytes to accumulate in the response. Note that this is not an absolute maximum, if the first
    # message in the first non-empty partition of the fetch is larger than this value, the message will
    # still be returned to ensure that progress can be made.
    max_bytes: "int"  # INT32

    # This setting controls the visibility of transactional records. Using READ_UNCOMMITTED
    # (isolation_level = 0) makes all records visible. With READ_COMMITTED (isolation_level = 1), non-
    # transactional and COMMITTED transactional records are visible. To be more concrete, READ_COMMITTED
    # returns all data from offsets smaller than the current LSO (last stable offset), and enables the
    # inclusion of the list of aborted transactions in the result, which allows consumers to discard
    # ABORTED transactional records
    isolation_level: "int"  # INT8

    # The fetch session ID
    session_id: "int"  # INT32

    # The fetch session epoch
    session_epoch: "int"  # INT32

    # Topics to fetch in the order provided.
    topics: List["Topics"]

    # Topics to remove from the fetch session.
    forgotten_topics_data: List["ForgottenTopicsData"]

    # The consumer's rack id
    rack_id: "str"  # STRING

    @staticmethod
    def api_key() -> int:
        return ApiKey.FETCH  # == 1


@dataclass
class AbortedTransactions:
    # The producer id associated with the aborted transactions
    producer_id: "int"  # INT64

    # The first offset in the aborted transaction
    first_offset: "int"  # INT64


@dataclass
class PartitionHeader:
    # Topic partition id
    partition: "int"  # INT32

    # Response error code
    error_code: "int"  # INT16

    # Last committed offset.
    high_watermark: "int"  # INT64

    # The last stable offset (or LSO) of the partition. This is the last offset such that the state of all
    # transactional records prior to this offset have been decided (ABORTED or COMMITTED)
    last_stable_offset: "int"  # INT64

    # Earliest available offset.
    log_start_offset: "int"  # INT64

    aborted_transactions: List["AbortedTransactions"]

    # The ID of the replica that the consumer should prefer.
    preferred_read_replica: "int"  # INT32


@dataclass
class PartitionResponses:
    partition_header: "PartitionHeader"

    record_set: "Records"  # RECORDS


@dataclass
class Responses:
    # Name of topic
    topic: "str"  # STRING

    partition_responses: List["PartitionResponses"]


@dataclass
class FetchResponseData(ResponseData):
    # Duration in milliseconds for which the request was throttled due to quota violation (Zero if the
    # request did not violate any quota)
    throttle_time_ms: "int"  # INT32

    # Response error code
    error_code: "int"  # INT16

    # The fetch session ID
    session_id: "int"  # INT32

    responses: List["Responses"]

    @staticmethod
    def api_key() -> int:
        return ApiKey.FETCH  # == 1


partitionsSchemas: Dict[int, Schema] = {
    0: [
        ("partition", int32Serializer),
        ("fetch_offset", int64Serializer),
        ("partition_max_bytes", int32Serializer),
        ("current_leader_epoch", DummySerializer(int())),
        ("log_start_offset", DummySerializer(int())),
    ],
    1: [
        ("partition", int32Serializer),
        ("fetch_offset", int64Serializer),
        ("partition_max_bytes", int32Serializer),
        ("current_leader_epoch", DummySerializer(int())),
        ("log_start_offset", DummySerializer(int())),
    ],
    2: [
        ("partition", int32Serializer),
        ("fetch_offset", int64Serializer),
        ("partition_max_bytes", int32Serializer),
        ("current_leader_epoch", DummySerializer(int())),
        ("log_start_offset", DummySerializer(int())),
    ],
    3: [
        ("partition", int32Serializer),
        ("fetch_offset", int64Serializer),
        ("partition_max_bytes", int32Serializer),
        ("current_leader_epoch", DummySerializer(int())),
        ("log_start_offset", DummySerializer(int())),
    ],
    4: [
        ("partition", int32Serializer),
        ("fetch_offset", int64Serializer),
        ("partition_max_bytes", int32Serializer),
        ("current_leader_epoch", DummySerializer(int())),
        ("log_start_offset", DummySerializer(int())),
    ],
    5: [
        ("partition", int32Serializer),
        ("fetch_offset", int64Serializer),
        ("log_start_offset", int64Serializer),
        ("partition_max_bytes", int32Serializer),
        ("current_leader_epoch", DummySerializer(int())),
    ],
    6: [
        ("partition", int32Serializer),
        ("fetch_offset", int64Serializer),
        ("log_start_offset", int64Serializer),
        ("partition_max_bytes", int32Serializer),
        ("current_leader_epoch", DummySerializer(int())),
    ],
    7: [
        ("partition", int32Serializer),
        ("fetch_offset", int64Serializer),
        ("log_start_offset", int64Serializer),
        ("partition_max_bytes", int32Serializer),
        ("current_leader_epoch", DummySerializer(int())),
    ],
    8: [
        ("partition", int32Serializer),
        ("fetch_offset", int64Serializer),
        ("log_start_offset", int64Serializer),
        ("partition_max_bytes", int32Serializer),
        ("current_leader_epoch", DummySerializer(int())),
    ],
    9: [
        ("partition", int32Serializer),
        ("current_leader_epoch", int32Serializer),
        ("fetch_offset", int64Serializer),
        ("log_start_offset", int64Serializer),
        ("partition_max_bytes", int32Serializer),
    ],
    10: [
        ("partition", int32Serializer),
        ("current_leader_epoch", int32Serializer),
        ("fetch_offset", int64Serializer),
        ("log_start_offset", int64Serializer),
        ("partition_max_bytes", int32Serializer),
    ],
    11: [
        ("partition", int32Serializer),
        ("current_leader_epoch", int32Serializer),
        ("fetch_offset", int64Serializer),
        ("log_start_offset", int64Serializer),
        ("partition_max_bytes", int32Serializer),
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
    4: [("topic", stringSerializer), ("partitions", ArraySerializer(partitionsSerializers[4]))],
    5: [("topic", stringSerializer), ("partitions", ArraySerializer(partitionsSerializers[5]))],
    6: [("topic", stringSerializer), ("partitions", ArraySerializer(partitionsSerializers[6]))],
    7: [("topic", stringSerializer), ("partitions", ArraySerializer(partitionsSerializers[7]))],
    8: [("topic", stringSerializer), ("partitions", ArraySerializer(partitionsSerializers[8]))],
    9: [("topic", stringSerializer), ("partitions", ArraySerializer(partitionsSerializers[9]))],
    10: [("topic", stringSerializer), ("partitions", ArraySerializer(partitionsSerializers[10]))],
    11: [("topic", stringSerializer), ("partitions", ArraySerializer(partitionsSerializers[11]))],
}


topicsSerializers: Dict[int, BaseSerializer[Topics]] = {
    version: NamedTupleSerializer(Topics, schema) for version, schema in topicsSchemas.items()
}


forgottenTopicsDataSchemas: Dict[int, Schema] = {
    7: [("topic", stringSerializer), ("partitions", ArraySerializer(int32Serializer))],
    8: [("topic", stringSerializer), ("partitions", ArraySerializer(int32Serializer))],
    9: [("topic", stringSerializer), ("partitions", ArraySerializer(int32Serializer))],
    10: [("topic", stringSerializer), ("partitions", ArraySerializer(int32Serializer))],
    11: [("topic", stringSerializer), ("partitions", ArraySerializer(int32Serializer))],
}


forgottenTopicsDataSerializers: Dict[int, BaseSerializer[ForgottenTopicsData]] = {
    version: NamedTupleSerializer(ForgottenTopicsData, schema)
    for version, schema in forgottenTopicsDataSchemas.items()
}


fetchRequestDataSchemas: Dict[int, Schema] = {
    0: [
        ("replica_id", int32Serializer),
        ("max_wait_time", int32Serializer),
        ("min_bytes", int32Serializer),
        ("topics", ArraySerializer(topicsSerializers[0])),
        ("max_bytes", DummySerializer(int())),
        ("isolation_level", DummySerializer(int())),
        ("session_id", DummySerializer(int())),
        ("session_epoch", DummySerializer(int())),
        ("forgotten_topics_data", DummySerializer([])),
        ("rack_id", DummySerializer(str())),
    ],
    1: [
        ("replica_id", int32Serializer),
        ("max_wait_time", int32Serializer),
        ("min_bytes", int32Serializer),
        ("topics", ArraySerializer(topicsSerializers[1])),
        ("max_bytes", DummySerializer(int())),
        ("isolation_level", DummySerializer(int())),
        ("session_id", DummySerializer(int())),
        ("session_epoch", DummySerializer(int())),
        ("forgotten_topics_data", DummySerializer([])),
        ("rack_id", DummySerializer(str())),
    ],
    2: [
        ("replica_id", int32Serializer),
        ("max_wait_time", int32Serializer),
        ("min_bytes", int32Serializer),
        ("topics", ArraySerializer(topicsSerializers[2])),
        ("max_bytes", DummySerializer(int())),
        ("isolation_level", DummySerializer(int())),
        ("session_id", DummySerializer(int())),
        ("session_epoch", DummySerializer(int())),
        ("forgotten_topics_data", DummySerializer([])),
        ("rack_id", DummySerializer(str())),
    ],
    3: [
        ("replica_id", int32Serializer),
        ("max_wait_time", int32Serializer),
        ("min_bytes", int32Serializer),
        ("max_bytes", int32Serializer),
        ("topics", ArraySerializer(topicsSerializers[3])),
        ("isolation_level", DummySerializer(int())),
        ("session_id", DummySerializer(int())),
        ("session_epoch", DummySerializer(int())),
        ("forgotten_topics_data", DummySerializer([])),
        ("rack_id", DummySerializer(str())),
    ],
    4: [
        ("replica_id", int32Serializer),
        ("max_wait_time", int32Serializer),
        ("min_bytes", int32Serializer),
        ("max_bytes", int32Serializer),
        ("isolation_level", int8Serializer),
        ("topics", ArraySerializer(topicsSerializers[4])),
        ("session_id", DummySerializer(int())),
        ("session_epoch", DummySerializer(int())),
        ("forgotten_topics_data", DummySerializer([])),
        ("rack_id", DummySerializer(str())),
    ],
    5: [
        ("replica_id", int32Serializer),
        ("max_wait_time", int32Serializer),
        ("min_bytes", int32Serializer),
        ("max_bytes", int32Serializer),
        ("isolation_level", int8Serializer),
        ("topics", ArraySerializer(topicsSerializers[5])),
        ("session_id", DummySerializer(int())),
        ("session_epoch", DummySerializer(int())),
        ("forgotten_topics_data", DummySerializer([])),
        ("rack_id", DummySerializer(str())),
    ],
    6: [
        ("replica_id", int32Serializer),
        ("max_wait_time", int32Serializer),
        ("min_bytes", int32Serializer),
        ("max_bytes", int32Serializer),
        ("isolation_level", int8Serializer),
        ("topics", ArraySerializer(topicsSerializers[6])),
        ("session_id", DummySerializer(int())),
        ("session_epoch", DummySerializer(int())),
        ("forgotten_topics_data", DummySerializer([])),
        ("rack_id", DummySerializer(str())),
    ],
    7: [
        ("replica_id", int32Serializer),
        ("max_wait_time", int32Serializer),
        ("min_bytes", int32Serializer),
        ("max_bytes", int32Serializer),
        ("isolation_level", int8Serializer),
        ("session_id", int32Serializer),
        ("session_epoch", int32Serializer),
        ("topics", ArraySerializer(topicsSerializers[7])),
        ("forgotten_topics_data", ArraySerializer(forgottenTopicsDataSerializers[7])),
        ("rack_id", DummySerializer(str())),
    ],
    8: [
        ("replica_id", int32Serializer),
        ("max_wait_time", int32Serializer),
        ("min_bytes", int32Serializer),
        ("max_bytes", int32Serializer),
        ("isolation_level", int8Serializer),
        ("session_id", int32Serializer),
        ("session_epoch", int32Serializer),
        ("topics", ArraySerializer(topicsSerializers[8])),
        ("forgotten_topics_data", ArraySerializer(forgottenTopicsDataSerializers[8])),
        ("rack_id", DummySerializer(str())),
    ],
    9: [
        ("replica_id", int32Serializer),
        ("max_wait_time", int32Serializer),
        ("min_bytes", int32Serializer),
        ("max_bytes", int32Serializer),
        ("isolation_level", int8Serializer),
        ("session_id", int32Serializer),
        ("session_epoch", int32Serializer),
        ("topics", ArraySerializer(topicsSerializers[9])),
        ("forgotten_topics_data", ArraySerializer(forgottenTopicsDataSerializers[9])),
        ("rack_id", DummySerializer(str())),
    ],
    10: [
        ("replica_id", int32Serializer),
        ("max_wait_time", int32Serializer),
        ("min_bytes", int32Serializer),
        ("max_bytes", int32Serializer),
        ("isolation_level", int8Serializer),
        ("session_id", int32Serializer),
        ("session_epoch", int32Serializer),
        ("topics", ArraySerializer(topicsSerializers[10])),
        ("forgotten_topics_data", ArraySerializer(forgottenTopicsDataSerializers[10])),
        ("rack_id", DummySerializer(str())),
    ],
    11: [
        ("replica_id", int32Serializer),
        ("max_wait_time", int32Serializer),
        ("min_bytes", int32Serializer),
        ("max_bytes", int32Serializer),
        ("isolation_level", int8Serializer),
        ("session_id", int32Serializer),
        ("session_epoch", int32Serializer),
        ("topics", ArraySerializer(topicsSerializers[11])),
        ("forgotten_topics_data", ArraySerializer(forgottenTopicsDataSerializers[11])),
        ("rack_id", stringSerializer),
    ],
}


fetchRequestDataSerializers: Dict[int, BaseSerializer[FetchRequestData]] = {
    version: NamedTupleSerializer(FetchRequestData, schema) for version, schema in fetchRequestDataSchemas.items()
}


abortedTransactionsSchemas: Dict[int, Schema] = {
    4: [("producer_id", int64Serializer), ("first_offset", int64Serializer)],
    5: [("producer_id", int64Serializer), ("first_offset", int64Serializer)],
    6: [("producer_id", int64Serializer), ("first_offset", int64Serializer)],
    7: [("producer_id", int64Serializer), ("first_offset", int64Serializer)],
    8: [("producer_id", int64Serializer), ("first_offset", int64Serializer)],
    9: [("producer_id", int64Serializer), ("first_offset", int64Serializer)],
    10: [("producer_id", int64Serializer), ("first_offset", int64Serializer)],
    11: [("producer_id", int64Serializer), ("first_offset", int64Serializer)],
}


abortedTransactionsSerializers: Dict[int, BaseSerializer[AbortedTransactions]] = {
    version: NamedTupleSerializer(AbortedTransactions, schema)
    for version, schema in abortedTransactionsSchemas.items()
}


partitionHeaderSchemas: Dict[int, Schema] = {
    0: [
        ("partition", int32Serializer),
        ("error_code", int16Serializer),
        ("high_watermark", int64Serializer),
        ("last_stable_offset", DummySerializer(int())),
        ("log_start_offset", DummySerializer(int())),
        ("aborted_transactions", DummySerializer([])),
        ("preferred_read_replica", DummySerializer(int())),
    ],
    1: [
        ("partition", int32Serializer),
        ("error_code", int16Serializer),
        ("high_watermark", int64Serializer),
        ("last_stable_offset", DummySerializer(int())),
        ("log_start_offset", DummySerializer(int())),
        ("aborted_transactions", DummySerializer([])),
        ("preferred_read_replica", DummySerializer(int())),
    ],
    2: [
        ("partition", int32Serializer),
        ("error_code", int16Serializer),
        ("high_watermark", int64Serializer),
        ("last_stable_offset", DummySerializer(int())),
        ("log_start_offset", DummySerializer(int())),
        ("aborted_transactions", DummySerializer([])),
        ("preferred_read_replica", DummySerializer(int())),
    ],
    3: [
        ("partition", int32Serializer),
        ("error_code", int16Serializer),
        ("high_watermark", int64Serializer),
        ("last_stable_offset", DummySerializer(int())),
        ("log_start_offset", DummySerializer(int())),
        ("aborted_transactions", DummySerializer([])),
        ("preferred_read_replica", DummySerializer(int())),
    ],
    4: [
        ("partition", int32Serializer),
        ("error_code", int16Serializer),
        ("high_watermark", int64Serializer),
        ("last_stable_offset", int64Serializer),
        ("aborted_transactions", ArraySerializer(abortedTransactionsSerializers[4])),
        ("log_start_offset", DummySerializer(int())),
        ("preferred_read_replica", DummySerializer(int())),
    ],
    5: [
        ("partition", int32Serializer),
        ("error_code", int16Serializer),
        ("high_watermark", int64Serializer),
        ("last_stable_offset", int64Serializer),
        ("log_start_offset", int64Serializer),
        ("aborted_transactions", ArraySerializer(abortedTransactionsSerializers[5])),
        ("preferred_read_replica", DummySerializer(int())),
    ],
    6: [
        ("partition", int32Serializer),
        ("error_code", int16Serializer),
        ("high_watermark", int64Serializer),
        ("last_stable_offset", int64Serializer),
        ("log_start_offset", int64Serializer),
        ("aborted_transactions", ArraySerializer(abortedTransactionsSerializers[6])),
        ("preferred_read_replica", DummySerializer(int())),
    ],
    7: [
        ("partition", int32Serializer),
        ("error_code", int16Serializer),
        ("high_watermark", int64Serializer),
        ("last_stable_offset", int64Serializer),
        ("log_start_offset", int64Serializer),
        ("aborted_transactions", ArraySerializer(abortedTransactionsSerializers[7])),
        ("preferred_read_replica", DummySerializer(int())),
    ],
    8: [
        ("partition", int32Serializer),
        ("error_code", int16Serializer),
        ("high_watermark", int64Serializer),
        ("last_stable_offset", int64Serializer),
        ("log_start_offset", int64Serializer),
        ("aborted_transactions", ArraySerializer(abortedTransactionsSerializers[8])),
        ("preferred_read_replica", DummySerializer(int())),
    ],
    9: [
        ("partition", int32Serializer),
        ("error_code", int16Serializer),
        ("high_watermark", int64Serializer),
        ("last_stable_offset", int64Serializer),
        ("log_start_offset", int64Serializer),
        ("aborted_transactions", ArraySerializer(abortedTransactionsSerializers[9])),
        ("preferred_read_replica", DummySerializer(int())),
    ],
    10: [
        ("partition", int32Serializer),
        ("error_code", int16Serializer),
        ("high_watermark", int64Serializer),
        ("last_stable_offset", int64Serializer),
        ("log_start_offset", int64Serializer),
        ("aborted_transactions", ArraySerializer(abortedTransactionsSerializers[10])),
        ("preferred_read_replica", DummySerializer(int())),
    ],
    11: [
        ("partition", int32Serializer),
        ("error_code", int16Serializer),
        ("high_watermark", int64Serializer),
        ("last_stable_offset", int64Serializer),
        ("log_start_offset", int64Serializer),
        ("aborted_transactions", ArraySerializer(abortedTransactionsSerializers[11])),
        ("preferred_read_replica", int32Serializer),
    ],
}


partitionHeaderSerializers: Dict[int, BaseSerializer[PartitionHeader]] = {
    version: NamedTupleSerializer(PartitionHeader, schema) for version, schema in partitionHeaderSchemas.items()
}


partitionResponsesSchemas: Dict[int, Schema] = {
    0: [("partition_header", partitionHeaderSerializers[0]), ("record_set", recordsSerializer)],
    1: [("partition_header", partitionHeaderSerializers[1]), ("record_set", recordsSerializer)],
    2: [("partition_header", partitionHeaderSerializers[2]), ("record_set", recordsSerializer)],
    3: [("partition_header", partitionHeaderSerializers[3]), ("record_set", recordsSerializer)],
    4: [("partition_header", partitionHeaderSerializers[4]), ("record_set", recordsSerializer)],
    5: [("partition_header", partitionHeaderSerializers[5]), ("record_set", recordsSerializer)],
    6: [("partition_header", partitionHeaderSerializers[6]), ("record_set", recordsSerializer)],
    7: [("partition_header", partitionHeaderSerializers[7]), ("record_set", recordsSerializer)],
    8: [("partition_header", partitionHeaderSerializers[8]), ("record_set", recordsSerializer)],
    9: [("partition_header", partitionHeaderSerializers[9]), ("record_set", recordsSerializer)],
    10: [("partition_header", partitionHeaderSerializers[10]), ("record_set", recordsSerializer)],
    11: [("partition_header", partitionHeaderSerializers[11]), ("record_set", recordsSerializer)],
}


partitionResponsesSerializers: Dict[int, BaseSerializer[PartitionResponses]] = {
    version: NamedTupleSerializer(PartitionResponses, schema) for version, schema in partitionResponsesSchemas.items()
}


responsesSchemas: Dict[int, Schema] = {
    0: [("topic", stringSerializer), ("partition_responses", ArraySerializer(partitionResponsesSerializers[0]))],
    1: [("topic", stringSerializer), ("partition_responses", ArraySerializer(partitionResponsesSerializers[1]))],
    2: [("topic", stringSerializer), ("partition_responses", ArraySerializer(partitionResponsesSerializers[2]))],
    3: [("topic", stringSerializer), ("partition_responses", ArraySerializer(partitionResponsesSerializers[3]))],
    4: [("topic", stringSerializer), ("partition_responses", ArraySerializer(partitionResponsesSerializers[4]))],
    5: [("topic", stringSerializer), ("partition_responses", ArraySerializer(partitionResponsesSerializers[5]))],
    6: [("topic", stringSerializer), ("partition_responses", ArraySerializer(partitionResponsesSerializers[6]))],
    7: [("topic", stringSerializer), ("partition_responses", ArraySerializer(partitionResponsesSerializers[7]))],
    8: [("topic", stringSerializer), ("partition_responses", ArraySerializer(partitionResponsesSerializers[8]))],
    9: [("topic", stringSerializer), ("partition_responses", ArraySerializer(partitionResponsesSerializers[9]))],
    10: [("topic", stringSerializer), ("partition_responses", ArraySerializer(partitionResponsesSerializers[10]))],
    11: [("topic", stringSerializer), ("partition_responses", ArraySerializer(partitionResponsesSerializers[11]))],
}


responsesSerializers: Dict[int, BaseSerializer[Responses]] = {
    version: NamedTupleSerializer(Responses, schema) for version, schema in responsesSchemas.items()
}


fetchResponseDataSchemas: Dict[int, Schema] = {
    0: [
        ("responses", ArraySerializer(responsesSerializers[0])),
        ("throttle_time_ms", DummySerializer(int())),
        ("error_code", DummySerializer(int())),
        ("session_id", DummySerializer(int())),
    ],
    1: [
        ("throttle_time_ms", int32Serializer),
        ("responses", ArraySerializer(responsesSerializers[1])),
        ("error_code", DummySerializer(int())),
        ("session_id", DummySerializer(int())),
    ],
    2: [
        ("throttle_time_ms", int32Serializer),
        ("responses", ArraySerializer(responsesSerializers[2])),
        ("error_code", DummySerializer(int())),
        ("session_id", DummySerializer(int())),
    ],
    3: [
        ("throttle_time_ms", int32Serializer),
        ("responses", ArraySerializer(responsesSerializers[3])),
        ("error_code", DummySerializer(int())),
        ("session_id", DummySerializer(int())),
    ],
    4: [
        ("throttle_time_ms", int32Serializer),
        ("responses", ArraySerializer(responsesSerializers[4])),
        ("error_code", DummySerializer(int())),
        ("session_id", DummySerializer(int())),
    ],
    5: [
        ("throttle_time_ms", int32Serializer),
        ("responses", ArraySerializer(responsesSerializers[5])),
        ("error_code", DummySerializer(int())),
        ("session_id", DummySerializer(int())),
    ],
    6: [
        ("throttle_time_ms", int32Serializer),
        ("responses", ArraySerializer(responsesSerializers[6])),
        ("error_code", DummySerializer(int())),
        ("session_id", DummySerializer(int())),
    ],
    7: [
        ("throttle_time_ms", int32Serializer),
        ("error_code", int16Serializer),
        ("session_id", int32Serializer),
        ("responses", ArraySerializer(responsesSerializers[7])),
    ],
    8: [
        ("throttle_time_ms", int32Serializer),
        ("error_code", int16Serializer),
        ("session_id", int32Serializer),
        ("responses", ArraySerializer(responsesSerializers[8])),
    ],
    9: [
        ("throttle_time_ms", int32Serializer),
        ("error_code", int16Serializer),
        ("session_id", int32Serializer),
        ("responses", ArraySerializer(responsesSerializers[9])),
    ],
    10: [
        ("throttle_time_ms", int32Serializer),
        ("error_code", int16Serializer),
        ("session_id", int32Serializer),
        ("responses", ArraySerializer(responsesSerializers[10])),
    ],
    11: [
        ("throttle_time_ms", int32Serializer),
        ("error_code", int16Serializer),
        ("session_id", int32Serializer),
        ("responses", ArraySerializer(responsesSerializers[11])),
    ],
}


fetchResponseDataSerializers: Dict[int, BaseSerializer[FetchResponseData]] = {
    version: NamedTupleSerializer(FetchResponseData, schema) for version, schema in fetchResponseDataSchemas.items()
}
