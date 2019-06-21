import functools
import itertools as it
import queue
import socket
from abc import ABCMeta, abstractstaticmethod
from concurrent import futures
from contextlib import contextmanager
from io import BytesIO
from typing import BinaryIO, Dict, Generic, List, NamedTuple, Optional, Set, Tuple, Type, TypeVar, overload

from esque.protocol.constants import ApiKey, ErrorCode
from esque.protocol.serializers import (
    ArraySerializer,
    BaseSerializer,
    DummySerializer,
    EnumSerializer,
    NamedTupleSerializer,
    Schema,
    int16Serializer,
    int32Serializer,
    nullableStringSerializer,
)

apiKeySerializer: BaseSerializer[ApiKey] = EnumSerializer(ApiKey, int16Serializer)

errorCodeSerializer: BaseSerializer[ErrorCode] = EnumSerializer(ErrorCode, int16Serializer)


class RequestHeader(NamedTuple):
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


class ResponseHeader(NamedTuple):
    correlation_id: int  # INT32


responseHeaderSchema: Schema = [("correlation_id", int32Serializer)]

responseHeaderSerializer: BaseSerializer[ResponseHeader] = NamedTupleSerializer(ResponseHeader, responseHeaderSchema)


class ApiSupportRange(NamedTuple):
    api_key: ApiKey
    min_version: int  # INT16
    max_version: int  # INT16


apiSupportRangeSchema: Schema = [
    ("api_key", apiKeySerializer),
    ("min_version", int16Serializer),
    ("max_version", int16Serializer),
]

apiSupportRangeSerializer: BaseSerializer[ApiSupportRange] = NamedTupleSerializer(
    ApiSupportRange, apiSupportRangeSchema
)


class RequestData(NamedTuple):
    @staticmethod
    def api_key() -> ApiKey:
        raise NotImplementedError()


class ResponseData(NamedTuple):
    pass


class ApiVersionRequestData(RequestData):
    @staticmethod
    def api_key() -> ApiKey:
        return ApiKey.API_VERSIONS


apiVersionRequestSchemas: Dict[int, Schema] = {0: [], 1: [], 2: []}


apiVersionRequestSerializers: Dict[int, BaseSerializer[ApiVersionRequestData]] = {
    version: NamedTupleSerializer(ApiVersionRequestData, schema) for version, schema in apiVersionRequestSchemas.items()
}


class ApiVersionResponseData(NamedTuple, ResponseData):
    error_code: ErrorCode  # INT16
    api_versions: List[ApiSupportRange]
    throttle_time_ms: Optional[int]  # INT32


apiVersionResponseSchemas: Dict[int, Schema] = {
    0: [
        ("error_code", errorCodeSerializer),
        ("api_versions", ArraySerializer(apiSupportRangeSerializer)),
        ("throttle_time_ms", DummySerializer(None)),
    ],
    1: [
        ("error_code", errorCodeSerializer),
        ("api_versions", ArraySerializer(apiSupportRangeSerializer)),
        ("throttle_time_ms", int32Serializer),
    ],
    2: [
        ("error_code", errorCodeSerializer),
        ("api_versions", ArraySerializer(apiSupportRangeSerializer)),
        ("throttle_time_ms", int32Serializer),
    ],
}

apiVersionResponseSerializers: Dict[int, BaseSerializer[ApiVersionResponseData]] = {
    version: NamedTupleSerializer(ApiVersionResponseData, schema)
    for version, schema in apiVersionResponseSchemas.items()
}
REQUEST_SERIALIZERS: Dict[ApiKey, Dict[int, BaseSerializer[RequestData]]] = {
    ApiKey.API_VERSIONS: apiVersionRequestSerializers
}
RESPONSE_SERIALIZERS: Dict[ApiKey, Dict[int, BaseSerializer[ResponseData]]] = {
    ApiKey.API_VERSIONS: apiVersionResponseSerializers
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

    def request_deserialize_done_cb(self, fut: futures.Future):
        self.response_header, self.response_data = fut.result()

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


class BrokerConnection:
    def __init__(self, address: Tuple[str, int], client_id: str):
        self.kafka_io = KafkaIO.from_address(address)
        self.client_id = client_id
        self._correlation_id_counter = it.count()
        self.api_versions: Dict[ApiKey, int] = {ApiKey.API_VERSIONS: 1}

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

        with self.kafka_io.no_blocking():
            while len(received_requests) < len_:

                self._try_send_and_pop(requests_to_send)
                try:
                    received_requests.append(self.kafka_io.receive())
                except queue.Empty:
                    pass
        return received_requests

    def _try_send_and_pop(self, requests_to_send: List[Request]) -> None:
        if requests_to_send:
            try:
                self.kafka_io.send(requests_to_send[0])
                del requests_to_send[0]

                if not requests_to_send:  # we're now empty, flush all messages
                    self.kafka_io.flush()
            except queue.Full:  # make sure we flush so some messages can be read to make place for new ones
                self.kafka_io.flush()

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
    def __init__(self, in_stream: BinaryIO, out_stream: BinaryIO, pool: Optional[futures.Executor] = None):
        self._in_flight: "queue.Queue[Request]" = queue.Queue()
        self._in_stream: BinaryIO = in_stream
        self._out_stream: BinaryIO = out_stream
        self._pool: Optional[futures.Executor] = pool
        self.blocking: bool = True

    def send(self, request: Request) -> None:
        data = request.encode_request()
        self._send_req_data(request, data)

    def _send_req_data(self, request: Request, data: bytes) -> None:
        self._in_flight.put(request, block=self.blocking)
        self._out_stream.write(int32Serializer.encode(len(data)))
        self._out_stream.write(data)

    def receive(self) -> Request:
        request, data = self._receive_req_data()
        request.decode_response(data)
        self._in_flight.task_done()
        return request

    def _receive_req_data(self) -> Tuple[Request, bytes]:
        request = self._in_flight.get(block=self.blocking)
        len_ = int32Serializer.read(self._in_stream)
        data = self._in_stream.read(len_)
        return request, data

    def flush(self):
        self._out_stream.flush()

    @contextmanager
    def no_blocking(self):
        self.blocking = False
        yield self
        self.blocking = True

    @classmethod
    def from_socket(cls, io_socket: socket.SocketType, *args, **kwargs) -> "KafkaIO":
        in_stream = io_socket.makefile(mode="rb", buffering=4096)
        out_stream = io_socket.makefile(mode="wb", buffering=4096)
        return cls(in_stream, out_stream, *args, **kwargs)

    @classmethod
    def from_address(cls, address: Tuple[str, int], *args, **kwargs) -> "KafkaIO":
        io_socket = socket.create_connection(address)
        return cls.from_socket(io_socket, *args, **kwargs)

    def close(self):
        self._in_stream.close()
        self._out_stream.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return self.close()


class AsyncKafkaIO(KafkaIO):
    def __init__(self, in_stream: BinaryIO, out_stream: BinaryIO, pool: Optional[futures.Executor]):
        super().__init__(in_stream, out_stream)
        self._pool: futures.Executor = pool
        self._receive_futures: Set[futures.Future] = set()
        self._send_futures: Set[futures.Future] = set()

    def receive_async(self) -> futures.Future:
        request, data = self._receive_req_data()
        fut: futures.Future = self._pool.submit(request.decode_response, data)
        self._receive_futures.add(fut)
        fut.add_done_callback(self._receive_futures.remove)
        return fut

    def send_async(self, request: Request) -> futures.Future:
        fut: futures.Future = self._pool.submit(request.encode_request)
        callback = functools.partial(self.serialization_done_cb, request=request)
        fut.add_done_callback(callback)
        return fut

    def serialization_done_cb(self, request: Request, fut: futures.Future):
        data = fut.result()
        self._send_req_data(request, data)
