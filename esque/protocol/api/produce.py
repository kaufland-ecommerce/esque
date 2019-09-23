# FIXME autogenerated module, check for errors!
from dataclasses import dataclass
from typing import Dict, Tuple, List, Optional

from esque.protocol.api.base import *
from esque.protocol.serializers import *


@dataclass
class Data:
    # Topic partition id
    partition: "int"  # INT32

    record_set: "Records"  # RECORDS


@dataclass
class TopicData:
    # Name of topic
    topic: "str"  # STRING

    data: List["Data"]


@dataclass
class ProduceRequestData(RequestData):
    # The transactional id or null if the producer is not transactional
    transactional_id: "Optional[str]"  # NULLABLE_STRING

    # The number of acknowledgments the producer requires the leader to have received before considering a
    # request complete. Allowed values: 0 for no acknowledgments, 1 for only the leader and -1 for the
    # full ISR.
    acks: "int"  # INT16

    # The time to await a response in ms.
    timeout: "int"  # INT32

    topic_data: List["TopicData"]

    @staticmethod
    def api_key() -> int:
        return ApiKey.PRODUCE  # == 0


@dataclass
class PartitionResponses:
    # Topic partition id
    partition: "int"  # INT32

    # Response error code
    error_code: "int"  # INT16

    base_offset: "int"  # INT64

    # The timestamp returned by broker after appending the messages. If CreateTime is used for the topic,
    # the timestamp will be -1. If LogAppendTime is used for the topic, the timestamp will be the broker
    # local time when the messages are appended.
    log_append_time: "int"  # INT64

    # The start offset of the log at the time this produce response was created
    log_start_offset: "int"  # INT64


@dataclass
class Responses:
    # Name of topic
    topic: "str"  # STRING

    partition_responses: List["PartitionResponses"]


@dataclass
class ProduceResponseData(ResponseData):
    responses: List["Responses"]

    # Duration in milliseconds for which the request was throttled due to quota violation (Zero if the
    # request did not violate any quota)
    throttle_time_ms: "int"  # INT32

    @staticmethod
    def api_key() -> int:
        return ApiKey.PRODUCE  # == 0


dataSchemas: Dict[int, Schema] = {
    0: [("partition", int32Serializer), ("record_set", recordsSerializer)],
    1: [("partition", int32Serializer), ("record_set", recordsSerializer)],
    2: [("partition", int32Serializer), ("record_set", recordsSerializer)],
    3: [("partition", int32Serializer), ("record_set", recordsSerializer)],
    4: [("partition", int32Serializer), ("record_set", recordsSerializer)],
    5: [("partition", int32Serializer), ("record_set", recordsSerializer)],
    6: [("partition", int32Serializer), ("record_set", recordsSerializer)],
    7: [("partition", int32Serializer), ("record_set", recordsSerializer)],
}


dataSerializers: Dict[int, BaseSerializer[Data]] = {
    version: NamedTupleSerializer(Data, schema) for version, schema in dataSchemas.items()
}


topicDataSchemas: Dict[int, Schema] = {
    0: [("topic", stringSerializer), ("data", ArraySerializer(dataSerializers[0]))],
    1: [("topic", stringSerializer), ("data", ArraySerializer(dataSerializers[1]))],
    2: [("topic", stringSerializer), ("data", ArraySerializer(dataSerializers[2]))],
    3: [("topic", stringSerializer), ("data", ArraySerializer(dataSerializers[3]))],
    4: [("topic", stringSerializer), ("data", ArraySerializer(dataSerializers[4]))],
    5: [("topic", stringSerializer), ("data", ArraySerializer(dataSerializers[5]))],
    6: [("topic", stringSerializer), ("data", ArraySerializer(dataSerializers[6]))],
    7: [("topic", stringSerializer), ("data", ArraySerializer(dataSerializers[7]))],
}


topicDataSerializers: Dict[int, BaseSerializer[TopicData]] = {
    version: NamedTupleSerializer(TopicData, schema) for version, schema in topicDataSchemas.items()
}


produceRequestDataSchemas: Dict[int, Schema] = {
    0: [
        ("acks", int16Serializer),
        ("timeout", int32Serializer),
        ("topic_data", ArraySerializer(topicDataSerializers[0])),
        ("transactional_id", DummySerializer(None)),
    ],
    1: [
        ("acks", int16Serializer),
        ("timeout", int32Serializer),
        ("topic_data", ArraySerializer(topicDataSerializers[1])),
        ("transactional_id", DummySerializer(None)),
    ],
    2: [
        ("acks", int16Serializer),
        ("timeout", int32Serializer),
        ("topic_data", ArraySerializer(topicDataSerializers[2])),
        ("transactional_id", DummySerializer(None)),
    ],
    3: [
        ("transactional_id", nullableStringSerializer),
        ("acks", int16Serializer),
        ("timeout", int32Serializer),
        ("topic_data", ArraySerializer(topicDataSerializers[3])),
    ],
    4: [
        ("transactional_id", nullableStringSerializer),
        ("acks", int16Serializer),
        ("timeout", int32Serializer),
        ("topic_data", ArraySerializer(topicDataSerializers[4])),
    ],
    5: [
        ("transactional_id", nullableStringSerializer),
        ("acks", int16Serializer),
        ("timeout", int32Serializer),
        ("topic_data", ArraySerializer(topicDataSerializers[5])),
    ],
    6: [
        ("transactional_id", nullableStringSerializer),
        ("acks", int16Serializer),
        ("timeout", int32Serializer),
        ("topic_data", ArraySerializer(topicDataSerializers[6])),
    ],
    7: [
        ("transactional_id", nullableStringSerializer),
        ("acks", int16Serializer),
        ("timeout", int32Serializer),
        ("topic_data", ArraySerializer(topicDataSerializers[7])),
    ],
}


produceRequestDataSerializers: Dict[int, BaseSerializer[ProduceRequestData]] = {
    version: NamedTupleSerializer(ProduceRequestData, schema) for version, schema in produceRequestDataSchemas.items()
}


partitionResponsesSchemas: Dict[int, Schema] = {
    0: [
        ("partition", int32Serializer),
        ("error_code", int16Serializer),
        ("base_offset", int64Serializer),
        ("log_append_time", DummySerializer(int())),
        ("log_start_offset", DummySerializer(int())),
    ],
    1: [
        ("partition", int32Serializer),
        ("error_code", int16Serializer),
        ("base_offset", int64Serializer),
        ("log_append_time", DummySerializer(int())),
        ("log_start_offset", DummySerializer(int())),
    ],
    2: [
        ("partition", int32Serializer),
        ("error_code", int16Serializer),
        ("base_offset", int64Serializer),
        ("log_append_time", int64Serializer),
        ("log_start_offset", DummySerializer(int())),
    ],
    3: [
        ("partition", int32Serializer),
        ("error_code", int16Serializer),
        ("base_offset", int64Serializer),
        ("log_append_time", int64Serializer),
        ("log_start_offset", DummySerializer(int())),
    ],
    4: [
        ("partition", int32Serializer),
        ("error_code", int16Serializer),
        ("base_offset", int64Serializer),
        ("log_append_time", int64Serializer),
        ("log_start_offset", DummySerializer(int())),
    ],
    5: [
        ("partition", int32Serializer),
        ("error_code", int16Serializer),
        ("base_offset", int64Serializer),
        ("log_append_time", int64Serializer),
        ("log_start_offset", int64Serializer),
    ],
    6: [
        ("partition", int32Serializer),
        ("error_code", int16Serializer),
        ("base_offset", int64Serializer),
        ("log_append_time", int64Serializer),
        ("log_start_offset", int64Serializer),
    ],
    7: [
        ("partition", int32Serializer),
        ("error_code", int16Serializer),
        ("base_offset", int64Serializer),
        ("log_append_time", int64Serializer),
        ("log_start_offset", int64Serializer),
    ],
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
}


responsesSerializers: Dict[int, BaseSerializer[Responses]] = {
    version: NamedTupleSerializer(Responses, schema) for version, schema in responsesSchemas.items()
}


produceResponseDataSchemas: Dict[int, Schema] = {
    0: [("responses", ArraySerializer(responsesSerializers[0])), ("throttle_time_ms", DummySerializer(int()))],
    1: [("responses", ArraySerializer(responsesSerializers[1])), ("throttle_time_ms", int32Serializer)],
    2: [("responses", ArraySerializer(responsesSerializers[2])), ("throttle_time_ms", int32Serializer)],
    3: [("responses", ArraySerializer(responsesSerializers[3])), ("throttle_time_ms", int32Serializer)],
    4: [("responses", ArraySerializer(responsesSerializers[4])), ("throttle_time_ms", int32Serializer)],
    5: [("responses", ArraySerializer(responsesSerializers[5])), ("throttle_time_ms", int32Serializer)],
    6: [("responses", ArraySerializer(responsesSerializers[6])), ("throttle_time_ms", int32Serializer)],
    7: [("responses", ArraySerializer(responsesSerializers[7])), ("throttle_time_ms", int32Serializer)],
}


produceResponseDataSerializers: Dict[int, BaseSerializer[ProduceResponseData]] = {
    version: NamedTupleSerializer(ProduceResponseData, schema)
    for version, schema in produceResponseDataSchemas.items()
}
