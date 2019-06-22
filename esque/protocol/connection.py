import functools
import itertools as it
import queue
import socket
from concurrent import futures
from contextlib import contextmanager
from typing import BinaryIO, Dict, List, Optional, Set, Tuple, overload

from .serializers import int32Serializer
from .api import ApiVersionRequestData, ApiVersionResponseData, Request, RequestData, ResponseData, ApiKey


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
