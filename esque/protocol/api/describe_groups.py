# FIXME autogenerated module, check for errors!
from dataclasses import dataclass
from typing import Dict, Tuple, List, Optional

from esque.protocol.api.base import *
from esque.protocol.serializers import *


@dataclass
class DescribeGroupsRequestData(RequestData):
    # The names of the groups to describe
    groups: List["str"]  # STRING

    # Whether to include authorized operations.
    include_authorized_operations: "bool"  # BOOLEAN

    @staticmethod
    def api_key() -> int:
        return ApiKey.DESCRIBE_GROUPS  # == 15


@dataclass
class Members:
    # The member ID assigned by the group coordinator.
    member_id: "str"  # STRING

    # The client ID used in the member's latest join group request.
    client_id: "str"  # STRING

    # The client host.
    client_host: "str"  # STRING

    # The metadata corresponding to the current group protocol in use.
    member_metadata: "bytes"  # BYTES

    # The current assignment provided by the group leader.
    member_assignment: "bytes"  # BYTES


@dataclass
class Groups:
    # The describe error, or 0 if there was no error.
    error_code: "int"  # INT16

    # The group ID string.
    group_id: "str"  # STRING

    # The group state string, or the empty string.
    group_state: "str"  # STRING

    # The group protocol type, or the empty string.
    protocol_type: "str"  # STRING

    # The group protocol data, or the empty string.
    protocol_data: "str"  # STRING

    # The group members.
    members: List["Members"]

    # 32-bit bitfield to represent authorized operations for this group.
    authorized_operations: "int"  # INT32


@dataclass
class DescribeGroupsResponseData(ResponseData):
    # The duration in milliseconds for which the request was throttled due to a quota violation, or zero
    # if the request did not violate any quota.
    throttle_time_ms: "int"  # INT32

    # Each described group.
    groups: List["Groups"]

    @staticmethod
    def api_key() -> int:
        return ApiKey.DESCRIBE_GROUPS  # == 15


describeGroupsRequestDataSchemas: Dict[int, Schema] = {
    0: [("groups", ArraySerializer(stringSerializer)), ("include_authorized_operations", DummySerializer(bool()))],
    1: [("groups", ArraySerializer(stringSerializer)), ("include_authorized_operations", DummySerializer(bool()))],
    2: [("groups", ArraySerializer(stringSerializer)), ("include_authorized_operations", DummySerializer(bool()))],
    3: [("groups", ArraySerializer(stringSerializer)), ("include_authorized_operations", booleanSerializer)],
}


describeGroupsRequestDataSerializers: Dict[int, BaseSerializer[DescribeGroupsRequestData]] = {
    version: NamedTupleSerializer(DescribeGroupsRequestData, schema)
    for version, schema in describeGroupsRequestDataSchemas.items()
}


membersSchemas: Dict[int, Schema] = {
    0: [
        ("member_id", stringSerializer),
        ("client_id", stringSerializer),
        ("client_host", stringSerializer),
        ("member_metadata", bytesSerializer),
        ("member_assignment", bytesSerializer),
    ],
    1: [
        ("member_id", stringSerializer),
        ("client_id", stringSerializer),
        ("client_host", stringSerializer),
        ("member_metadata", bytesSerializer),
        ("member_assignment", bytesSerializer),
    ],
    2: [
        ("member_id", stringSerializer),
        ("client_id", stringSerializer),
        ("client_host", stringSerializer),
        ("member_metadata", bytesSerializer),
        ("member_assignment", bytesSerializer),
    ],
    3: [
        ("member_id", stringSerializer),
        ("client_id", stringSerializer),
        ("client_host", stringSerializer),
        ("member_metadata", bytesSerializer),
        ("member_assignment", bytesSerializer),
    ],
}


membersSerializers: Dict[int, BaseSerializer[Members]] = {
    version: NamedTupleSerializer(Members, schema) for version, schema in membersSchemas.items()
}


groupsSchemas: Dict[int, Schema] = {
    0: [
        ("error_code", int16Serializer),
        ("group_id", stringSerializer),
        ("group_state", stringSerializer),
        ("protocol_type", stringSerializer),
        ("protocol_data", stringSerializer),
        ("members", ArraySerializer(membersSerializers[0])),
        ("authorized_operations", DummySerializer(int())),
    ],
    1: [
        ("error_code", int16Serializer),
        ("group_id", stringSerializer),
        ("group_state", stringSerializer),
        ("protocol_type", stringSerializer),
        ("protocol_data", stringSerializer),
        ("members", ArraySerializer(membersSerializers[1])),
        ("authorized_operations", DummySerializer(int())),
    ],
    2: [
        ("error_code", int16Serializer),
        ("group_id", stringSerializer),
        ("group_state", stringSerializer),
        ("protocol_type", stringSerializer),
        ("protocol_data", stringSerializer),
        ("members", ArraySerializer(membersSerializers[2])),
        ("authorized_operations", DummySerializer(int())),
    ],
    3: [
        ("error_code", int16Serializer),
        ("group_id", stringSerializer),
        ("group_state", stringSerializer),
        ("protocol_type", stringSerializer),
        ("protocol_data", stringSerializer),
        ("members", ArraySerializer(membersSerializers[3])),
        ("authorized_operations", int32Serializer),
    ],
}


groupsSerializers: Dict[int, BaseSerializer[Groups]] = {
    version: NamedTupleSerializer(Groups, schema) for version, schema in groupsSchemas.items()
}


describeGroupsResponseDataSchemas: Dict[int, Schema] = {
    0: [("groups", ArraySerializer(groupsSerializers[0])), ("throttle_time_ms", DummySerializer(int()))],
    1: [("throttle_time_ms", int32Serializer), ("groups", ArraySerializer(groupsSerializers[1]))],
    2: [("throttle_time_ms", int32Serializer), ("groups", ArraySerializer(groupsSerializers[2]))],
    3: [("throttle_time_ms", int32Serializer), ("groups", ArraySerializer(groupsSerializers[3]))],
}


describeGroupsResponseDataSerializers: Dict[int, BaseSerializer[DescribeGroupsResponseData]] = {
    version: NamedTupleSerializer(DescribeGroupsResponseData, schema)
    for version, schema in describeGroupsResponseDataSchemas.items()
}
