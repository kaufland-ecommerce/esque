from io import BytesIO
from typing import BinaryIO, Dict, Generic, Optional, TypeVar

from esque.protocol.api.base import (
    ApiKey,
    RequestData,
    RequestHeader,
    ResponseData,
    ResponseHeader,
    requestHeaderSerializer,
    responseHeaderSerializer,
)
from esque.protocol.serializers import BaseSerializer
from .add_offsets_to_txn import (
    AddOffsetsToTxnRequestData,
    AddOffsetsToTxnResponseData,
    addOffsetsToTxnRequestDataSerializers,
    addOffsetsToTxnResponseDataSerializers,
)
from .add_partitions_to_txn import (
    AddPartitionsToTxnRequestData,
    AddPartitionsToTxnResponseData,
    addPartitionsToTxnRequestDataSerializers,
    addPartitionsToTxnResponseDataSerializers,
)
from .alter_configs import (
    AlterConfigsRequestData,
    AlterConfigsResponseData,
    alterConfigsRequestDataSerializers,
    alterConfigsResponseDataSerializers,
)
from .alter_replica_log_dirs import (
    AlterReplicaLogDirsRequestData,
    AlterReplicaLogDirsResponseData,
    alterReplicaLogDirsRequestDataSerializers,
    alterReplicaLogDirsResponseDataSerializers,
)
from .api_versions import (
    ApiVersions,
    ApiVersionsRequestData,
    ApiVersionsResponseData,
    apiVersionsRequestDataSerializers,
    apiVersionsResponseDataSerializers,
)
from .controlled_shutdown import (
    ControlledShutdownRequestData,
    ControlledShutdownResponseData,
    controlledShutdownRequestDataSerializers,
    controlledShutdownResponseDataSerializers,
)
from .create_acls import (
    CreateAclsRequestData,
    CreateAclsResponseData,
    createAclsRequestDataSerializers,
    createAclsResponseDataSerializers,
)
from .create_delegation_token import (
    CreateDelegationTokenRequestData,
    CreateDelegationTokenResponseData,
    createDelegationTokenRequestDataSerializers,
    createDelegationTokenResponseDataSerializers,
)
from .create_partitions import (
    CreatePartitionsRequestData,
    CreatePartitionsResponseData,
    createPartitionsRequestDataSerializers,
    createPartitionsResponseDataSerializers,
)
from .create_topics import (
    CreateTopicsRequestData,
    CreateTopicsResponseData,
    createTopicsRequestDataSerializers,
    createTopicsResponseDataSerializers,
)
from .delete_acls import (
    DeleteAclsRequestData,
    DeleteAclsResponseData,
    deleteAclsRequestDataSerializers,
    deleteAclsResponseDataSerializers,
)
from .delete_groups import (
    DeleteGroupsRequestData,
    DeleteGroupsResponseData,
    deleteGroupsRequestDataSerializers,
    deleteGroupsResponseDataSerializers,
)
from .delete_records import (
    DeleteRecordsRequestData,
    DeleteRecordsResponseData,
    deleteRecordsRequestDataSerializers,
    deleteRecordsResponseDataSerializers,
)
from .delete_topics import (
    DeleteTopicsRequestData,
    DeleteTopicsResponseData,
    deleteTopicsRequestDataSerializers,
    deleteTopicsResponseDataSerializers,
)
from .describe_acls import (
    DescribeAclsRequestData,
    DescribeAclsResponseData,
    describeAclsRequestDataSerializers,
    describeAclsResponseDataSerializers,
)
from .describe_configs import (
    DescribeConfigsRequestData,
    DescribeConfigsResponseData,
    describeConfigsRequestDataSerializers,
    describeConfigsResponseDataSerializers,
)
from .describe_delegation_token import (
    DescribeDelegationTokenRequestData,
    DescribeDelegationTokenResponseData,
    describeDelegationTokenRequestDataSerializers,
    describeDelegationTokenResponseDataSerializers,
)
from .describe_groups import (
    DescribeGroupsRequestData,
    DescribeGroupsResponseData,
    describeGroupsRequestDataSerializers,
    describeGroupsResponseDataSerializers,
)
from .describe_log_dirs import (
    DescribeLogDirsRequestData,
    DescribeLogDirsResponseData,
    describeLogDirsRequestDataSerializers,
    describeLogDirsResponseDataSerializers,
)
from .elect_preferred_leaders import (
    ElectPreferredLeadersRequestData,
    ElectPreferredLeadersResponseData,
    electPreferredLeadersRequestDataSerializers,
    electPreferredLeadersResponseDataSerializers,
)
from .end_txn import EndTxnRequestData, EndTxnResponseData, endTxnRequestDataSerializers, endTxnResponseDataSerializers
from .expire_delegation_token import (
    ExpireDelegationTokenRequestData,
    ExpireDelegationTokenResponseData,
    expireDelegationTokenRequestDataSerializers,
    expireDelegationTokenResponseDataSerializers,
)
from .fetch import FetchRequestData, FetchResponseData, fetchRequestDataSerializers, fetchResponseDataSerializers
from .find_coordinator import (
    FindCoordinatorRequestData,
    FindCoordinatorResponseData,
    findCoordinatorRequestDataSerializers,
    findCoordinatorResponseDataSerializers,
)
from .heartbeat import (
    HeartbeatRequestData,
    HeartbeatResponseData,
    heartbeatRequestDataSerializers,
    heartbeatResponseDataSerializers,
)
from .incremental_alter_configs import (
    IncrementalAlterConfigsRequestData,
    IncrementalAlterConfigsResponseData,
    incrementalAlterConfigsRequestDataSerializers,
    incrementalAlterConfigsResponseDataSerializers,
)
from .init_producer_id import (
    InitProducerIdRequestData,
    InitProducerIdResponseData,
    initProducerIdRequestDataSerializers,
    initProducerIdResponseDataSerializers,
)
from .join_group import (
    JoinGroupRequestData,
    JoinGroupResponseData,
    joinGroupRequestDataSerializers,
    joinGroupResponseDataSerializers,
)
from .leader_and_isr import (
    LeaderAndIsrRequestData,
    LeaderAndIsrResponseData,
    leaderAndIsrRequestDataSerializers,
    leaderAndIsrResponseDataSerializers,
)
from .leave_group import (
    LeaveGroupRequestData,
    LeaveGroupResponseData,
    leaveGroupRequestDataSerializers,
    leaveGroupResponseDataSerializers,
)
from .list_groups import (
    ListGroupsRequestData,
    ListGroupsResponseData,
    listGroupsRequestDataSerializers,
    listGroupsResponseDataSerializers,
)
from .list_offsets import (
    ListOffsetsRequestData,
    ListOffsetsResponseData,
    listOffsetsRequestDataSerializers,
    listOffsetsResponseDataSerializers,
)
from .metadata import (
    MetadataRequestData,
    MetadataResponseData,
    metadataRequestDataSerializers,
    metadataResponseDataSerializers,
)
from .offset_commit import (
    OffsetCommitRequestData,
    OffsetCommitResponseData,
    offsetCommitRequestDataSerializers,
    offsetCommitResponseDataSerializers,
)
from .offset_fetch import (
    OffsetFetchRequestData,
    OffsetFetchResponseData,
    offsetFetchRequestDataSerializers,
    offsetFetchResponseDataSerializers,
)
from .offset_for_leader_epoch import (
    OffsetForLeaderEpochRequestData,
    OffsetForLeaderEpochResponseData,
    offsetForLeaderEpochRequestDataSerializers,
    offsetForLeaderEpochResponseDataSerializers,
)
from .produce import (
    ProduceRequestData,
    ProduceResponseData,
    produceRequestDataSerializers,
    produceResponseDataSerializers,
)
from .renew_delegation_token import (
    RenewDelegationTokenRequestData,
    RenewDelegationTokenResponseData,
    renewDelegationTokenRequestDataSerializers,
    renewDelegationTokenResponseDataSerializers,
)
from .sasl_authenticate import (
    SaslAuthenticateRequestData,
    SaslAuthenticateResponseData,
    saslAuthenticateRequestDataSerializers,
    saslAuthenticateResponseDataSerializers,
)
from .sasl_handshake import (
    SaslHandshakeRequestData,
    SaslHandshakeResponseData,
    saslHandshakeRequestDataSerializers,
    saslHandshakeResponseDataSerializers,
)
from .stop_replica import (
    StopReplicaRequestData,
    StopReplicaResponseData,
    stopReplicaRequestDataSerializers,
    stopReplicaResponseDataSerializers,
)
from .sync_group import (
    SyncGroupRequestData,
    SyncGroupResponseData,
    syncGroupRequestDataSerializers,
    syncGroupResponseDataSerializers,
)
from .txn_offset_commit import (
    TxnOffsetCommitRequestData,
    TxnOffsetCommitResponseData,
    txnOffsetCommitRequestDataSerializers,
    txnOffsetCommitResponseDataSerializers,
)
from .update_metadata import (
    UpdateMetadataRequestData,
    UpdateMetadataResponseData,
    updateMetadataRequestDataSerializers,
    updateMetadataResponseDataSerializers,
)
from .write_txn_markers import (
    WriteTxnMarkersRequestData,
    WriteTxnMarkersResponseData,
    writeTxnMarkersRequestDataSerializers,
    writeTxnMarkersResponseDataSerializers,
)

REQUEST_SERIALIZERS: Dict[ApiKey, Dict[int, BaseSerializer[RequestData]]] = {
    ApiKey.PRODUCE: produceRequestDataSerializers,
    ApiKey.FETCH: fetchRequestDataSerializers,
    ApiKey.LIST_OFFSETS: listOffsetsRequestDataSerializers,
    ApiKey.METADATA: metadataRequestDataSerializers,
    ApiKey.LEADER_AND_ISR: leaderAndIsrRequestDataSerializers,
    ApiKey.STOP_REPLICA: stopReplicaRequestDataSerializers,
    ApiKey.UPDATE_METADATA: updateMetadataRequestDataSerializers,
    ApiKey.CONTROLLED_SHUTDOWN: controlledShutdownRequestDataSerializers,
    ApiKey.OFFSET_COMMIT: offsetCommitRequestDataSerializers,
    ApiKey.OFFSET_FETCH: offsetFetchRequestDataSerializers,
    ApiKey.FIND_COORDINATOR: findCoordinatorRequestDataSerializers,
    ApiKey.JOIN_GROUP: joinGroupRequestDataSerializers,
    ApiKey.HEARTBEAT: heartbeatRequestDataSerializers,
    ApiKey.LEAVE_GROUP: leaveGroupRequestDataSerializers,
    ApiKey.SYNC_GROUP: syncGroupRequestDataSerializers,
    ApiKey.DESCRIBE_GROUPS: describeGroupsRequestDataSerializers,
    ApiKey.LIST_GROUPS: listGroupsRequestDataSerializers,
    ApiKey.SASL_HANDSHAKE: saslHandshakeRequestDataSerializers,
    ApiKey.API_VERSIONS: apiVersionsRequestDataSerializers,
    ApiKey.CREATE_TOPICS: createTopicsRequestDataSerializers,
    ApiKey.DELETE_TOPICS: deleteTopicsRequestDataSerializers,
    ApiKey.DELETE_RECORDS: deleteRecordsRequestDataSerializers,
    ApiKey.INIT_PRODUCER_ID: initProducerIdRequestDataSerializers,
    ApiKey.OFFSET_FOR_LEADER_EPOCH: offsetForLeaderEpochRequestDataSerializers,
    ApiKey.ADD_PARTITIONS_TO_TXN: addPartitionsToTxnRequestDataSerializers,
    ApiKey.ADD_OFFSETS_TO_TXN: addOffsetsToTxnRequestDataSerializers,
    ApiKey.END_TXN: endTxnRequestDataSerializers,
    ApiKey.WRITE_TXN_MARKERS: writeTxnMarkersRequestDataSerializers,
    ApiKey.TXN_OFFSET_COMMIT: txnOffsetCommitRequestDataSerializers,
    ApiKey.DESCRIBE_ACLS: describeAclsRequestDataSerializers,
    ApiKey.CREATE_ACLS: createAclsRequestDataSerializers,
    ApiKey.DELETE_ACLS: deleteAclsRequestDataSerializers,
    ApiKey.DESCRIBE_CONFIGS: describeConfigsRequestDataSerializers,
    ApiKey.ALTER_CONFIGS: alterConfigsRequestDataSerializers,
    ApiKey.ALTER_REPLICA_LOG_DIRS: alterReplicaLogDirsRequestDataSerializers,
    ApiKey.DESCRIBE_LOG_DIRS: describeLogDirsRequestDataSerializers,
    ApiKey.SASL_AUTHENTICATE: saslAuthenticateRequestDataSerializers,
    ApiKey.CREATE_PARTITIONS: createPartitionsRequestDataSerializers,
    ApiKey.CREATE_DELEGATION_TOKEN: createDelegationTokenRequestDataSerializers,
    ApiKey.RENEW_DELEGATION_TOKEN: renewDelegationTokenRequestDataSerializers,
    ApiKey.EXPIRE_DELEGATION_TOKEN: expireDelegationTokenRequestDataSerializers,
    ApiKey.DESCRIBE_DELEGATION_TOKEN: describeDelegationTokenRequestDataSerializers,
    ApiKey.DELETE_GROUPS: deleteGroupsRequestDataSerializers,
    ApiKey.ELECT_PREFERRED_LEADERS: electPreferredLeadersRequestDataSerializers,
    ApiKey.INCREMENTAL_ALTER_CONFIGS: incrementalAlterConfigsRequestDataSerializers,
}


RESPONSE_SERIALIZERS: Dict[ApiKey, Dict[int, BaseSerializer[ResponseData]]] = {
    ApiKey.PRODUCE: produceResponseDataSerializers,
    ApiKey.FETCH: fetchResponseDataSerializers,
    ApiKey.LIST_OFFSETS: listOffsetsResponseDataSerializers,
    ApiKey.METADATA: metadataResponseDataSerializers,
    ApiKey.LEADER_AND_ISR: leaderAndIsrResponseDataSerializers,
    ApiKey.STOP_REPLICA: stopReplicaResponseDataSerializers,
    ApiKey.UPDATE_METADATA: updateMetadataResponseDataSerializers,
    ApiKey.CONTROLLED_SHUTDOWN: controlledShutdownResponseDataSerializers,
    ApiKey.OFFSET_COMMIT: offsetCommitResponseDataSerializers,
    ApiKey.OFFSET_FETCH: offsetFetchResponseDataSerializers,
    ApiKey.FIND_COORDINATOR: findCoordinatorResponseDataSerializers,
    ApiKey.JOIN_GROUP: joinGroupResponseDataSerializers,
    ApiKey.HEARTBEAT: heartbeatResponseDataSerializers,
    ApiKey.LEAVE_GROUP: leaveGroupResponseDataSerializers,
    ApiKey.SYNC_GROUP: syncGroupResponseDataSerializers,
    ApiKey.DESCRIBE_GROUPS: describeGroupsResponseDataSerializers,
    ApiKey.LIST_GROUPS: listGroupsResponseDataSerializers,
    ApiKey.SASL_HANDSHAKE: saslHandshakeResponseDataSerializers,
    ApiKey.API_VERSIONS: apiVersionsResponseDataSerializers,
    ApiKey.CREATE_TOPICS: createTopicsResponseDataSerializers,
    ApiKey.DELETE_TOPICS: deleteTopicsResponseDataSerializers,
    ApiKey.DELETE_RECORDS: deleteRecordsResponseDataSerializers,
    ApiKey.INIT_PRODUCER_ID: initProducerIdResponseDataSerializers,
    ApiKey.OFFSET_FOR_LEADER_EPOCH: offsetForLeaderEpochResponseDataSerializers,
    ApiKey.ADD_PARTITIONS_TO_TXN: addPartitionsToTxnResponseDataSerializers,
    ApiKey.ADD_OFFSETS_TO_TXN: addOffsetsToTxnResponseDataSerializers,
    ApiKey.END_TXN: endTxnResponseDataSerializers,
    ApiKey.WRITE_TXN_MARKERS: writeTxnMarkersResponseDataSerializers,
    ApiKey.TXN_OFFSET_COMMIT: txnOffsetCommitResponseDataSerializers,
    ApiKey.DESCRIBE_ACLS: describeAclsResponseDataSerializers,
    ApiKey.CREATE_ACLS: createAclsResponseDataSerializers,
    ApiKey.DELETE_ACLS: deleteAclsResponseDataSerializers,
    ApiKey.DESCRIBE_CONFIGS: describeConfigsResponseDataSerializers,
    ApiKey.ALTER_CONFIGS: alterConfigsResponseDataSerializers,
    ApiKey.ALTER_REPLICA_LOG_DIRS: alterReplicaLogDirsResponseDataSerializers,
    ApiKey.DESCRIBE_LOG_DIRS: describeLogDirsResponseDataSerializers,
    ApiKey.SASL_AUTHENTICATE: saslAuthenticateResponseDataSerializers,
    ApiKey.CREATE_PARTITIONS: createPartitionsResponseDataSerializers,
    ApiKey.CREATE_DELEGATION_TOKEN: createDelegationTokenResponseDataSerializers,
    ApiKey.RENEW_DELEGATION_TOKEN: renewDelegationTokenResponseDataSerializers,
    ApiKey.EXPIRE_DELEGATION_TOKEN: expireDelegationTokenResponseDataSerializers,
    ApiKey.DESCRIBE_DELEGATION_TOKEN: describeDelegationTokenResponseDataSerializers,
    ApiKey.DELETE_GROUPS: deleteGroupsResponseDataSerializers,
    ApiKey.ELECT_PREFERRED_LEADERS: electPreferredLeadersResponseDataSerializers,
    ApiKey.INCREMENTAL_ALTER_CONFIGS: incrementalAlterConfigsResponseDataSerializers,
}


SUPPORTED_API_VERSIONS: Dict[ApiKey, ApiVersions] = {
    api_key: ApiVersions(api_key, min(serializers.keys()), max(serializers.keys()))
    for api_key, serializers in REQUEST_SERIALIZERS.items()
}


def get_request_serializer(api_key: ApiKey, api_version: int) -> BaseSerializer[RequestData]:
    return REQUEST_SERIALIZERS[api_key][api_version]


def get_response_serializer(api_key: ApiKey, api_version: int) -> BaseSerializer[ResponseData]:
    return RESPONSE_SERIALIZERS[api_key][api_version]


Req = TypeVar("Req")
Res = TypeVar("Res")


class Request(Generic[Req, Res]):
    def __init__(self, request_data: Req, header: RequestHeader):
        self.api_version = header.api_version
        self.request_data = request_data
        self.request_header = header
        self.response_data: Optional[Res] = None
        self.response_header: Optional[ResponseHeader] = None

    def encode_request(self) -> bytes:
        data = requestHeaderSerializer.encode(self.request_header)
        data += self.request_serializer.encode(self.request_data)
        return data

    def decode_response(self, data: bytes) -> "Request":
        return self.read_response(BytesIO(data))

    def read_response(self, buffer: BinaryIO) -> "Request":
        self.response_header = responseHeaderSerializer.read(buffer)
        assert self.response_header.correlation_id == self.correlation_id, "Request and response order got messed up!"
        self.response_data = self.response_serializer.read(buffer)
        return self

    @property
    def correlation_id(self) -> int:
        return self.request_header.correlation_id

    @property
    def api_key(self) -> ApiKey:
        return self.request_header.api_key

    @property
    def response_serializer(self) -> BaseSerializer[Res]:
        return get_response_serializer(self.api_key, self.api_version)

    @property
    def request_serializer(self) -> BaseSerializer[Req]:
        return get_request_serializer(self.api_key, self.api_version)

    @classmethod
    def from_request_data(
        cls, request_data: Req, api_version: int, correlation_id: int, client_id: Optional[str]
    ) -> "Request":
        request_data = request_data
        header = RequestHeader(
            api_key=request_data.api_key(), api_version=api_version, correlation_id=correlation_id, client_id=client_id
        )
        return Request(request_data, header)
