import itertools as it
import queue
import socket
from typing import BinaryIO, Dict, List, Tuple, overload

from .api import (
    ApiKey,
    ApiVersionRequestData,
    ApiVersionResponseData,
    ApiSupportRange,
    Request,
    RequestData,
    ResponseData,
    SUPPORTED_API_VERSIONS,
)
from .serializers import int32Serializer


class BrokerConnection:
    def __init__(self, address: Tuple[str, int], client_id: str):
        self.kafka_io = KafkaIO.from_address(address)
        self.client_id = client_id
        self._correlation_id_counter = it.count()
        self.api_versions: Dict[ApiKey, int] = {ApiKey.API_VERSIONS: 1}
        self._query_api_versions()

    def _query_api_versions(self) -> None:
        request = self.send(ApiVersionRequestData())
        all_server_supported_versions = {support_range.api_key: support_range for
                                         support_range in request.response_data.api_versions}
        server_api_keys = set(all_server_supported_versions)
        client_api_keys = set(SUPPORTED_API_VERSIONS)
        for api_key in server_api_keys | client_api_keys:
            client_supported_version = SUPPORTED_API_VERSIONS.get(api_key, ApiSupportRange(api_key, -2, -1))
            server_supported_version = all_server_supported_versions.get(api_key, ApiSupportRange(api_key, -4, -3))
            effective_version = min(client_supported_version.max_version, server_supported_version.max_version)

            # TODO messages say something like server only supports api ... up to version -4
            #  better say server doesn't support api ... PERIOD
            # I'd like to do warings/exceptions during runtime once a feature is actually needed. This makes sure the
            # client can be used for everything where the api versions match and/or are high enough.
            # In the high level part, I imagine function annotations like @requires(ApiKey.LIST_OFFSETS, 2) if a
            # function requires the server to support api LIST_OFFSETS of at least version 2
            if client_supported_version.min_version <= effective_version:
                raise Warning(
                    f"Server only supports api {api_key.name} up to version {server_supported_version.max_version}," +
                    f"but client needs at least {client_supported_version.min_version}. You cannot use this API."
                )
            if server_supported_version.min_version <= effective_version:
                raise Warning(
                    f"Client only supports api {api_key.name} up to version {client_supported_version.max_version}," +
                    f"but server needs at least {server_supported_version.min_version}. You cannot use this API."
                )
            self.api_versions[api_key] = effective_version

    @overload
    def send(self, data: ApiVersionRequestData) -> Request[ApiVersionRequestData, ApiVersionResponseData]:
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
