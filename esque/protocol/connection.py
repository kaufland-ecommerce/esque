# noqa: F811,F401

import itertools as it
import queue
import socket
import warnings
from typing import BinaryIO, Dict, List, Tuple, overload

from .api import (
    ApiKey,
    ApiVersions,
    Request,
    RequestData,
    ResponseData,
    SUPPORTED_API_VERSIONS,
    ProduceRequestData,
    ProduceResponseData,
    FetchRequestData,
    FetchResponseData,
    ListOffsetsRequestData,
    ListOffsetsResponseData,
    MetadataRequestData,
    MetadataResponseData,
    LeaderAndIsrRequestData,
    LeaderAndIsrResponseData,
    StopReplicaRequestData,
    StopReplicaResponseData,
    UpdateMetadataRequestData,
    UpdateMetadataResponseData,
    ControlledShutdownRequestData,
    ControlledShutdownResponseData,
    OffsetCommitRequestData,
    OffsetCommitResponseData,
    OffsetFetchRequestData,
    OffsetFetchResponseData,
    FindCoordinatorRequestData,
    FindCoordinatorResponseData,
    JoinGroupRequestData,
    JoinGroupResponseData,
    HeartbeatRequestData,
    HeartbeatResponseData,
    LeaveGroupRequestData,
    LeaveGroupResponseData,
    SyncGroupRequestData,
    SyncGroupResponseData,
    DescribeGroupsRequestData,
    DescribeGroupsResponseData,
    ListGroupsRequestData,
    ListGroupsResponseData,
    SaslHandshakeRequestData,
    SaslHandshakeResponseData,
    ApiVersionsRequestData,
    ApiVersionsResponseData,
    CreateTopicsRequestData,
    CreateTopicsResponseData,
    DeleteTopicsRequestData,
    DeleteTopicsResponseData,
    DeleteRecordsRequestData,
    DeleteRecordsResponseData,
    InitProducerIdRequestData,
    InitProducerIdResponseData,
    OffsetForLeaderEpochRequestData,
    OffsetForLeaderEpochResponseData,
    AddPartitionsToTxnRequestData,
    AddPartitionsToTxnResponseData,
    AddOffsetsToTxnRequestData,
    AddOffsetsToTxnResponseData,
    EndTxnRequestData,
    EndTxnResponseData,
    WriteTxnMarkersRequestData,
    WriteTxnMarkersResponseData,
    TxnOffsetCommitRequestData,
    TxnOffsetCommitResponseData,
    DescribeAclsRequestData,
    DescribeAclsResponseData,
    CreateAclsRequestData,
    CreateAclsResponseData,
    DeleteAclsRequestData,
    DeleteAclsResponseData,
    DescribeConfigsRequestData,
    DescribeConfigsResponseData,
    AlterConfigsRequestData,
    AlterConfigsResponseData,
    AlterReplicaLogDirsRequestData,
    AlterReplicaLogDirsResponseData,
    DescribeLogDirsRequestData,
    DescribeLogDirsResponseData,
    SaslAuthenticateRequestData,
    SaslAuthenticateResponseData,
    CreatePartitionsRequestData,
    CreatePartitionsResponseData,
    CreateDelegationTokenRequestData,
    CreateDelegationTokenResponseData,
    RenewDelegationTokenRequestData,
    RenewDelegationTokenResponseData,
    ExpireDelegationTokenRequestData,
    ExpireDelegationTokenResponseData,
    DescribeDelegationTokenRequestData,
    DescribeDelegationTokenResponseData,
    DeleteGroupsRequestData,
    DeleteGroupsResponseData,
    ElectPreferredLeadersRequestData,
    ElectPreferredLeadersResponseData,
    IncrementalAlterConfigsRequestData,
    IncrementalAlterConfigsResponseData,
)
from .serializers import int32Serializer


class ApiNotSupportedWarning(UserWarning):
    pass


class BrokerConnection:
    def __init__(self, address: Tuple[str, int], client_id: str):
        self.kafka_io = KafkaIO.from_address(address)
        self.client_id = client_id
        self._correlation_id_counter = it.count()
        self.api_versions: Dict[ApiKey, int] = {ApiKey.API_VERSIONS: 1}
        self._query_api_versions()

    def _query_api_versions(self) -> None:
        request = self.send(ApiVersionsRequestData())
        all_server_supported_versions = {
            ApiKey(support_range.api_key): support_range for support_range in request.response_data.api_versions
        }
        server_api_keys = set(all_server_supported_versions)
        client_api_keys = set(SUPPORTED_API_VERSIONS)
        for api_key in server_api_keys | client_api_keys:
            client_supported_version = SUPPORTED_API_VERSIONS.get(api_key, ApiVersions(api_key, -2, -1))
            server_supported_version = all_server_supported_versions.get(api_key, ApiVersions(api_key, -4, -3))
            effective_version = min(client_supported_version.max_version, server_supported_version.max_version)

            # TODO messages say something like server only supports api ... up to version -4
            #  better say server doesn't support api ... PERIOD
            # I'd like to do warings/exceptions during runtime once a feature is actually needed. This makes sure the
            # client can be used for everything where the api versions match and/or are high enough.
            # In the high level part, I imagine function annotations like @requires(ApiKey.LIST_OFFSETS, 2) if a
            # function requires the server to support api LIST_OFFSETS of at least version 2
            if effective_version < client_supported_version.min_version:
                if server_supported_version.max_version == -3:
                    warnings.warn(
                        ApiNotSupportedWarning(
                            f"Client supports API {api_key.name} up to version {client_supported_version.max_version}, "
                            + f"but server does not support the API at all. You cannot use this API."
                        )
                    )
                else:
                    warnings.warn(
                        ApiNotSupportedWarning(
                            f"Server only supports API {api_key.name} up to version"
                            f"{server_supported_version.max_version}, but client needs at least "
                            f"{client_supported_version.min_version}. You cannot use this API."
                        )
                    )
            if effective_version < server_supported_version.min_version:
                if client_supported_version.max_version == -1:
                    warnings.warn(
                        ApiNotSupportedWarning(
                            f"Server supports api {api_key.name} up to version {server_supported_version.max_version}, "
                            + f"but client does not support the API at all. You cannot use this API."
                        )
                    )
                else:
                    warnings.warn(
                        ApiNotSupportedWarning(
                            f"Client only supports API {api_key.name} up to version"
                            f"{client_supported_version.max_version}, but server needs at least "
                            f"{server_supported_version.min_version}. You cannot use this API."
                        )
                    )
            self.api_versions[api_key] = effective_version

    @overload
    def send(self, data: ProduceRequestData) -> Request[ProduceRequestData, ProduceResponseData]:
        ...

    @overload
    def send(self, data: FetchRequestData) -> Request[FetchRequestData, FetchResponseData]:
        ...

    @overload
    def send(self, data: ListOffsetsRequestData) -> Request[ListOffsetsRequestData, ListOffsetsResponseData]:
        ...

    @overload
    def send(self, data: MetadataRequestData) -> Request[MetadataRequestData, MetadataResponseData]:
        ...

    @overload
    def send(self, data: LeaderAndIsrRequestData) -> Request[LeaderAndIsrRequestData, LeaderAndIsrResponseData]:
        ...

    @overload
    def send(self, data: StopReplicaRequestData) -> Request[StopReplicaRequestData, StopReplicaResponseData]:
        ...

    @overload
    def send(self, data: UpdateMetadataRequestData) -> Request[UpdateMetadataRequestData, UpdateMetadataResponseData]:
        ...

    @overload
    def send(
        self, data: ControlledShutdownRequestData
    ) -> Request[ControlledShutdownRequestData, ControlledShutdownResponseData]:
        ...

    @overload
    def send(self, data: OffsetCommitRequestData) -> Request[OffsetCommitRequestData, OffsetCommitResponseData]:
        ...

    @overload
    def send(self, data: OffsetFetchRequestData) -> Request[OffsetFetchRequestData, OffsetFetchResponseData]:
        ...

    @overload
    def send(
        self, data: FindCoordinatorRequestData
    ) -> Request[FindCoordinatorRequestData, FindCoordinatorResponseData]:
        ...

    @overload
    def send(self, data: JoinGroupRequestData) -> Request[JoinGroupRequestData, JoinGroupResponseData]:
        ...

    @overload
    def send(self, data: HeartbeatRequestData) -> Request[HeartbeatRequestData, HeartbeatResponseData]:
        ...

    @overload
    def send(self, data: LeaveGroupRequestData) -> Request[LeaveGroupRequestData, LeaveGroupResponseData]:
        ...

    @overload
    def send(self, data: SyncGroupRequestData) -> Request[SyncGroupRequestData, SyncGroupResponseData]:
        ...

    @overload
    def send(self, data: DescribeGroupsRequestData) -> Request[DescribeGroupsRequestData, DescribeGroupsResponseData]:
        ...

    @overload
    def send(self, data: ListGroupsRequestData) -> Request[ListGroupsRequestData, ListGroupsResponseData]:
        ...

    @overload
    def send(self, data: SaslHandshakeRequestData) -> Request[SaslHandshakeRequestData, SaslHandshakeResponseData]:
        ...

    @overload
    def send(self, data: ApiVersionsRequestData) -> Request[ApiVersionsRequestData, ApiVersionsResponseData]:
        ...

    @overload
    def send(self, data: CreateTopicsRequestData) -> Request[CreateTopicsRequestData, CreateTopicsResponseData]:
        ...

    @overload
    def send(self, data: DeleteTopicsRequestData) -> Request[DeleteTopicsRequestData, DeleteTopicsResponseData]:
        ...

    @overload
    def send(self, data: DeleteRecordsRequestData) -> Request[DeleteRecordsRequestData, DeleteRecordsResponseData]:
        ...

    @overload
    def send(self, data: InitProducerIdRequestData) -> Request[InitProducerIdRequestData, InitProducerIdResponseData]:
        ...

    @overload
    def send(
        self, data: OffsetForLeaderEpochRequestData
    ) -> Request[OffsetForLeaderEpochRequestData, OffsetForLeaderEpochResponseData]:
        ...

    @overload
    def send(
        self, data: AddPartitionsToTxnRequestData
    ) -> Request[AddPartitionsToTxnRequestData, AddPartitionsToTxnResponseData]:
        ...

    @overload
    def send(
        self, data: AddOffsetsToTxnRequestData
    ) -> Request[AddOffsetsToTxnRequestData, AddOffsetsToTxnResponseData]:
        ...

    @overload
    def send(self, data: EndTxnRequestData) -> Request[EndTxnRequestData, EndTxnResponseData]:
        ...

    @overload
    def send(
        self, data: WriteTxnMarkersRequestData
    ) -> Request[WriteTxnMarkersRequestData, WriteTxnMarkersResponseData]:
        ...

    @overload
    def send(
        self, data: TxnOffsetCommitRequestData
    ) -> Request[TxnOffsetCommitRequestData, TxnOffsetCommitResponseData]:
        ...

    @overload
    def send(self, data: DescribeAclsRequestData) -> Request[DescribeAclsRequestData, DescribeAclsResponseData]:
        ...

    @overload
    def send(self, data: CreateAclsRequestData) -> Request[CreateAclsRequestData, CreateAclsResponseData]:
        ...

    @overload
    def send(self, data: DeleteAclsRequestData) -> Request[DeleteAclsRequestData, DeleteAclsResponseData]:
        ...

    @overload
    def send(
        self, data: DescribeConfigsRequestData
    ) -> Request[DescribeConfigsRequestData, DescribeConfigsResponseData]:
        ...

    @overload
    def send(self, data: AlterConfigsRequestData) -> Request[AlterConfigsRequestData, AlterConfigsResponseData]:
        ...

    @overload
    def send(
        self, data: AlterReplicaLogDirsRequestData
    ) -> Request[AlterReplicaLogDirsRequestData, AlterReplicaLogDirsResponseData]:
        ...

    @overload
    def send(
        self, data: DescribeLogDirsRequestData
    ) -> Request[DescribeLogDirsRequestData, DescribeLogDirsResponseData]:
        ...

    @overload
    def send(
        self, data: SaslAuthenticateRequestData
    ) -> Request[SaslAuthenticateRequestData, SaslAuthenticateResponseData]:
        ...

    @overload
    def send(
        self, data: CreatePartitionsRequestData
    ) -> Request[CreatePartitionsRequestData, CreatePartitionsResponseData]:
        ...

    @overload
    def send(
        self, data: CreateDelegationTokenRequestData
    ) -> Request[CreateDelegationTokenRequestData, CreateDelegationTokenResponseData]:
        ...

    @overload
    def send(
        self, data: RenewDelegationTokenRequestData
    ) -> Request[RenewDelegationTokenRequestData, RenewDelegationTokenResponseData]:
        ...

    @overload
    def send(
        self, data: ExpireDelegationTokenRequestData
    ) -> Request[ExpireDelegationTokenRequestData, ExpireDelegationTokenResponseData]:
        ...

    @overload
    def send(
        self, data: DescribeDelegationTokenRequestData
    ) -> Request[DescribeDelegationTokenRequestData, DescribeDelegationTokenResponseData]:
        ...

    @overload
    def send(self, data: DeleteGroupsRequestData) -> Request[DeleteGroupsRequestData, DeleteGroupsResponseData]:
        ...

    @overload
    def send(
        self, data: ElectPreferredLeadersRequestData
    ) -> Request[ElectPreferredLeadersRequestData, ElectPreferredLeadersResponseData]:
        ...

    @overload
    def send(
        self, data: IncrementalAlterConfigsRequestData
    ) -> Request[IncrementalAlterConfigsRequestData, IncrementalAlterConfigsResponseData]:
        ...

    def send(self, request_data: RequestData) -> Request[RequestData, ResponseData]:
        return self.send_many([request_data])[0]

    def send_many(self, request_data_to_send: List[RequestData]) -> List[Request]:
        requests_to_send = [self._request_from_data(data) for data in request_data_to_send]

        received_requests: List[Request] = []

        len_ = len(request_data_to_send)
        if len_ == 0:
            return []

        while len(received_requests) < len_:
            self._try_send_and_pop_from(requests_to_send)
            self._try_receive_and_append_to(received_requests)
        return received_requests

    def _try_send_and_pop_from(self, requests_to_send: List[Request]) -> None:
        if requests_to_send:
            try:
                self.kafka_io.send(requests_to_send[0])
                del requests_to_send[0]

                if not requests_to_send:  # we're now empty, flush all messages
                    self.kafka_io.flush()
            except queue.Full:  # make sure we flush so some messages can be read to make place for new ones
                self.kafka_io.flush()

    def _try_receive_and_append_to(self, received_requests: List[Request]) -> None:
        try:
            received_requests.append(self.kafka_io.receive())
        except queue.Empty:
            pass

    def _request_from_data(self, request_data: RequestData) -> Request:
        api_key = request_data.api_key()
        api_version = self.api_versions[api_key]
        return Request.from_request_data(request_data, api_version, next(self._correlation_id_counter), self.client_id)

    def close(self):
        self.kafka_io.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return self.close()


class KafkaIO:
    def __init__(self, in_stream: BinaryIO, out_stream: BinaryIO):
        self._in_flight: "queue.Queue[Request]" = queue.Queue(maxsize=10)  # TODO make this configurable
        self._in_stream: BinaryIO = in_stream
        self._out_stream: BinaryIO = out_stream

    def send(self, request: Request) -> None:
        data = request.encode_request()
        self._send_req_data(request, data)

    def _send_req_data(self, request: Request, data: bytes) -> None:
        self._in_flight.put(request, block=False)
        self._out_stream.write(int32Serializer.encode(len(data)))
        self._out_stream.write(data)

    def receive(self) -> Request:
        request, data = self._receive_req_data()
        request.decode_response(data)
        self._in_flight.task_done()
        return request

    def _receive_req_data(self) -> Tuple[Request, bytes]:
        request = self._in_flight.get(block=False)
        len_ = int32Serializer.read(self._in_stream)
        data = self._in_stream.read(len_)
        return request, data

    def flush(self):
        self._out_stream.flush()

    @classmethod
    def from_socket(cls, io_socket: socket.SocketType) -> "KafkaIO":
        in_stream = io_socket.makefile(mode="rb", buffering=4096)
        out_stream = io_socket.makefile(mode="wb", buffering=4096)
        return cls(in_stream, out_stream)

    @classmethod
    def from_address(cls, address: Tuple[str, int]) -> "KafkaIO":
        io_socket = socket.create_connection(address)
        return cls.from_socket(io_socket)

    def close(self):
        self._in_stream.close()
        self._out_stream.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return self.close()
