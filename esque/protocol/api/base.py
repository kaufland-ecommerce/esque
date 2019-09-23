from enum import IntEnum
from typing import Optional

from dataclasses import dataclass

from ..constants import ErrorCode
from ..serializers import (
    BaseSerializer,
    EnumSerializer,
    NamedTupleSerializer,
    Schema,
    int16Serializer,
    int32Serializer,
    nullableStringSerializer,
)


class ApiKey(IntEnum):
    PRODUCE = 0
    FETCH = 1
    LIST_OFFSETS = 2
    METADATA = 3
    LEADER_AND_ISR = 4
    STOP_REPLICA = 5
    UPDATE_METADATA = 6
    CONTROLLED_SHUTDOWN = 7
    OFFSET_COMMIT = 8
    OFFSET_FETCH = 9
    FIND_COORDINATOR = 10
    JOIN_GROUP = 11
    HEARTBEAT = 12
    LEAVE_GROUP = 13
    SYNC_GROUP = 14
    DESCRIBE_GROUPS = 15
    LIST_GROUPS = 16
    SASL_HANDSHAKE = 17
    API_VERSIONS = 18
    CREATE_TOPICS = 19
    DELETE_TOPICS = 20
    DELETE_RECORDS = 21
    INIT_PRODUCER_ID = 22
    OFFSET_FOR_LEADER_EPOCH = 23
    ADD_PARTITIONS_TO_TXN = 24
    ADD_OFFSETS_TO_TXN = 25
    END_TXN = 26
    WRITE_TXN_MARKERS = 27
    TXN_OFFSET_COMMIT = 28
    DESCRIBE_ACLS = 29
    CREATE_ACLS = 30
    DELETE_ACLS = 31
    DESCRIBE_CONFIGS = 32
    ALTER_CONFIGS = 33
    ALTER_REPLICA_LOG_DIRS = 34
    DESCRIBE_LOG_DIRS = 35
    SASL_AUTHENTICATE = 36
    CREATE_PARTITIONS = 37
    CREATE_DELEGATION_TOKEN = 38
    RENEW_DELEGATION_TOKEN = 39
    EXPIRE_DELEGATION_TOKEN = 40
    DESCRIBE_DELEGATION_TOKEN = 41
    DELETE_GROUPS = 42
    ELECT_PREFERRED_LEADERS = 43
    INCREMENTAL_ALTER_CONFIGS = 44


apiKeySerializer: BaseSerializer[ApiKey] = EnumSerializer(ApiKey, int16Serializer)

errorCodeSerializer: BaseSerializer[ErrorCode] = EnumSerializer(ErrorCode, int16Serializer)


@dataclass
class RequestHeader:
    api_key: ApiKey
    api_version: int  # INT16
    correlation_id: int  # INT32
    client_id: Optional[str]


requestHeaderSchema: Schema = [
    ("api_key", apiKeySerializer),
    ("api_version", int16Serializer),
    ("correlation_id", int32Serializer),
    ("client_id", nullableStringSerializer),
]

requestHeaderSerializer: BaseSerializer[RequestHeader] = NamedTupleSerializer(RequestHeader, requestHeaderSchema)


@dataclass
class ResponseHeader:
    correlation_id: int  # INT32


responseHeaderSchema: Schema = [("correlation_id", int32Serializer)]

responseHeaderSerializer: BaseSerializer[ResponseHeader] = NamedTupleSerializer(ResponseHeader, responseHeaderSchema)


@dataclass
class RequestData:
    @staticmethod
    def api_key() -> ApiKey:
        raise NotImplementedError()


@dataclass
class ResponseData:
    @staticmethod
    def api_key() -> ApiKey:
        raise NotImplementedError()
