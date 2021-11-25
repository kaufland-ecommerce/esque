import enum
import functools
import urllib.parse
from abc import ABC, abstractmethod
from contextlib import closing
from typing import Callable, ClassVar, Dict, Iterable, List, NamedTuple, Optional, Tuple, Union

from esque.io.exceptions import EsqueIOInvalidPipelineBuilderState, ExqueIOInvalidURIException
from esque.io.handlers import BaseHandler, PipeHandler, create_handler
from esque.io.handlers.pipe import PipeHandlerConfig
from esque.io.messages import Message
from esque.io.serializers import StringSerializer, create_serializer
from esque.io.serializers.base import MessageSerializer
from esque.io.serializers.string import StringSerializerConfig
from esque.io.stream_decorators import stop_after_nth_message
from esque.io.stream_events import StreamEvent


class MessageReader(ABC):
    @abstractmethod
    def read_message(self) -> Message:
        raise NotImplementedError

    @abstractmethod
    def message_stream(self) -> Iterable[Message]:
        raise NotImplementedError

    @abstractmethod
    def seek(self, position: int) -> None:
        raise NotImplementedError

    @abstractmethod
    def close(self) -> None:
        raise NotImplementedError


class _Schemes(NamedTuple):
    handler_scheme: str
    key_serializer_scheme: str
    value_serializer_scheme: str


class UriConfig:
    handler_config: Dict[str, str]
    key_serializer_config: Dict[str, str]
    value_serializer_config: Dict[str, str]
    errors: List[str]
    _parsed_uri: urllib.parse.ParseResult

    KEY_PARAM_PREFIX: ClassVar[str] = "k__"
    VALUE_PARAM_PREFIX: ClassVar[str] = "v__"
    KEYVALUE_PARAM_PREFIX: ClassVar[str] = "kv__"
    HANDLER_PARAM_PREFIX: ClassVar[str] = "h__"

    ALL_PREFIXES: ClassVar[List[str]] = [
        KEY_PARAM_PREFIX,
        VALUE_PARAM_PREFIX,
        KEYVALUE_PARAM_PREFIX,
        HANDLER_PARAM_PREFIX,
    ]

    def __init__(self, uri: str):
        self.handler_config = {}
        self.key_serializer_config = {}
        self.value_serializer_config = {}
        self.errors = []
        self._parsed_uri: urllib.parse.ParseResult = urllib.parse.urlparse(uri)
        self._evaluate_uri()

    def _evaluate_uri(self):
        self._evaluate_uri_scheme()
        self._assign_handler_host_and_path()
        self._evaluate_query_params()
        if self.errors:
            raise ExqueIOInvalidURIException("Invalid URI:\n" + "\n".join(self.errors))

    def _evaluate_uri_scheme(self):
        schemes = self._parse_scheme(self._parsed_uri.scheme)
        self._assign_schemes(schemes)

    def _parse_scheme(self, scheme: str) -> _Schemes:
        parts = scheme.split("+")
        if len(parts) == 1:
            self.errors.append(f"Missing serializer scheme, only got handler scheme {parts[0]!r}.")
            parts.extend(["missing"] * 2)
        if len(parts) == 2:
            # If only one serializer scheme is provided, that means both key and value serializer use the same
            parts.append(parts[1])
        return _Schemes(*parts)

    def _assign_schemes(self, schemes):
        self.handler_config["scheme"] = schemes.handler_scheme
        self.key_serializer_config["scheme"] = schemes.key_serializer_scheme
        self.value_serializer_config["scheme"] = schemes.value_serializer_scheme

    def _assign_handler_host_and_path(self):
        host: str = self._parsed_uri.netloc
        path: str = self._parsed_uri.path[1:]
        self.handler_config.update({"host": host, "path": path})

    def _evaluate_query_params(self):
        parsed_params: Dict[str, List[str]] = urllib.parse.parse_qs(self._parsed_uri.query, keep_blank_values=True)
        for key, values in parsed_params.items():
            self._add_raw_param(key, values)

    def _add_raw_param(self, key: str, values: List[str]):
        if len(values) > 1:
            self.errors.append(f"Multiple parameter values for {key!r}: {values!r}")
            return

        value = values[0]
        prefix, real_key = self._strip_prefix(key)
        self._add_param(prefix, real_key, value)

    def _strip_prefix(self, key: str) -> Tuple[str, str]:
        for prefix in self.ALL_PREFIXES:
            if key.startswith(prefix):
                return prefix, key[len(prefix) :]

    def _add_param(self, prefix: str, key: str, value: str):
        if prefix == self.HANDLER_PARAM_PREFIX:
            self.handler_config[key] = value
        elif prefix == self.KEY_PARAM_PREFIX:
            self.key_serializer_config[key] = value
        elif prefix == self.VALUE_PARAM_PREFIX:
            self.value_serializer_config[key] = value
        elif prefix == self.KEYVALUE_PARAM_PREFIX:
            self.key_serializer_config[key] = value
            self.value_serializer_config[key] = value


class MessageWriter(ABC):
    @abstractmethod
    def write_message(self, message: Message):
        raise NotImplementedError

    @abstractmethod
    def write_many_messages(self, message_stream: Iterable[Union[Message, StreamEvent]]):
        raise NotImplementedError

    @abstractmethod
    def close(self) -> None:
        raise NotImplementedError


class HandlerSerializerMessageReader(MessageReader):
    _handler: BaseHandler
    _message_serializer: MessageSerializer

    def __init__(self, handler: BaseHandler, message_serializer: MessageSerializer):
        self._handler = handler
        self._message_serializer = message_serializer

    def read_message(self) -> Union[Message, StreamEvent]:
        msg = self._handler.read_message()
        if isinstance(msg, StreamEvent):
            return msg
        return self._message_serializer.deserialize(binary_message=msg)

    def message_stream(self) -> Iterable[Message]:
        return self._message_serializer.deserialize_many(binary_message_stream=self._handler.binary_message_stream())

    def seek(self, position: int):
        self._handler.seek(position)

    def close(self):
        self._handler.close()


class HandlerSerializerMessageWriter(MessageWriter):
    _handler: BaseHandler
    _message_serializer: MessageSerializer

    def __init__(self, handler: BaseHandler, message_serializer: MessageSerializer):
        self._handler = handler
        self._message_serializer = message_serializer

    def write_message(self, message: Message):
        self._handler.write_message(binary_message=self._message_serializer.serialize(message=message))

    def write_many_messages(self, message_stream: Iterable[Union[Message, StreamEvent]]):
        self._handler.write_many_messages(
            message_stream=self._message_serializer.serialize_many(messages=message_stream)
        )

    def close(self):
        self._handler.close()


class Pipeline:
    _input_element: MessageReader
    _output_element: MessageWriter
    _stream_decorators: List[Callable[[Iterable], Iterable]]

    def __init__(
        self,
        input_element: MessageReader,
        output_element: MessageWriter,
        stream_decorators: List[Callable[[Iterable], Iterable]],
    ):
        self._input_element = input_element
        self._output_element = output_element
        self._stream_decorators = stream_decorators

    def message_stream(self) -> Iterable:
        return self._input_element.message_stream()

    def decorated_message_stream(self) -> Iterable:
        stream = self.message_stream()
        for decorator in self._stream_decorators:
            stream = decorator(stream)
        return stream

    def run_pipeline(self):
        with closing(self):
            self._output_element.write_many_messages(self.decorated_message_stream())

    def write_message(self, message: Message):
        self._output_element.write_message(message=message)

    def write_many_messages(self, message_stream: Iterable[Union[Message, StreamEvent]]):
        self._output_element.write_many_messages(message_stream=message_stream)

    def close(self) -> None:
        self._input_element.close()
        self._output_element.close()


class _BuilderComponentState(enum.Flag):
    NOTHING_DEFINED = 0
    URI_DEFINED = enum.auto()
    SERIALIZER_DEFINED = enum.auto()
    HANDLER_DEFINED = enum.auto()
    READER_WRITER_DEFINED = enum.auto()
    HANDLER_SERIALIZER_DEFINED = HANDLER_DEFINED | SERIALIZER_DEFINED

    def is_valid(self) -> bool:
        return self in {
            _BuilderComponentState.NOTHING_DEFINED,
            _BuilderComponentState.URI_DEFINED,
            _BuilderComponentState.READER_WRITER_DEFINED,
            _BuilderComponentState.HANDLER_SERIALIZER_DEFINED,
        }


class PipelineBuilder:
    _input_handler: Optional[BaseHandler] = None
    _input_serializer: Optional[MessageSerializer] = None
    _message_reader: Optional[MessageReader] = None
    _input_uri: Optional[str] = None
    _input_state: _BuilderComponentState = _BuilderComponentState.NOTHING_DEFINED

    _output_handler: Optional[BaseHandler] = None
    _output_serializer: Optional[MessageSerializer] = None
    _message_writer: Optional[MessageWriter] = None
    _output_uri: Optional[str] = None
    _output_state: _BuilderComponentState = _BuilderComponentState.NOTHING_DEFINED

    _stream_decorators: List[Callable[[Iterable], Iterable]]
    _start: Optional[int] = None
    _errors: List[str]

    def __init__(self):
        """
        Creates a new PipelineBuilder. In case no methods are called other than :meth:`PipelineBuilder.build()`, the created pipeline
        will have a pair of console handlers (stdin and stdout) and UTF-8 string message serializers.
        """
        self._stream_decorators = []
        self._errors = []
        self._start: Optional[int] = None
        self._limit: Optional[int] = None

    def with_input_handler(self, handler: BaseHandler) -> "PipelineBuilder":
        if handler is not None:
            self._input_handler = handler
            self._input_state |= _BuilderComponentState.HANDLER_DEFINED
        return self

    def with_input_message_serializer(self, serializer: MessageSerializer) -> "PipelineBuilder":
        if serializer is not None:
            self._input_serializer = serializer
            self._input_state |= _BuilderComponentState.SERIALIZER_DEFINED
        return self

    def with_message_reader(self, message_reader: MessageReader) -> "PipelineBuilder":
        if message_reader is not None:
            self._message_reader = message_reader
            self._input_state |= _BuilderComponentState.READER_WRITER_DEFINED
        return self

    def with_output_handler(self, handler: BaseHandler) -> "PipelineBuilder":
        if handler is not None:
            self._output_handler = handler
            self._output_state |= _BuilderComponentState.HANDLER_DEFINED
        return self

    def with_output_message_serializer(self, serializer: MessageSerializer) -> "PipelineBuilder":
        if serializer is not None:
            self._output_serializer = serializer
            self._output_state |= _BuilderComponentState.SERIALIZER_DEFINED
        return self

    def with_message_writer(self, message_writer: MessageWriter) -> "PipelineBuilder":
        if message_writer is not None:
            self._message_writer = message_writer
            self._output_state |= _BuilderComponentState.READER_WRITER_DEFINED
        return self

    def with_stream_decorator(self, decorator: Callable[[Iterable], Iterable]) -> "PipelineBuilder":
        if decorator is not None:
            self._stream_decorators.append(decorator)
        return self

    def with_input_from_uri(self, uri: str) -> "PipelineBuilder":
        if uri is not None:
            self._input_uri = uri
            self._input_state |= _BuilderComponentState.URI_DEFINED
        return self

    def with_output_from_uri(self, uri: str) -> "PipelineBuilder":
        if uri is not None:
            self._output_uri = uri
            self._output_state |= _BuilderComponentState.URI_DEFINED
        return self

    def add_transformation(self, transformation) -> "PipelineBuilder":
        raise NotImplementedError

    @functools.cached_property
    def _pipeline(self) -> Pipeline:
        message_reader = self._build_message_reader()
        message_writer = self._build_message_writer()
        self._add_limit_decorator()
        if self._errors:
            raise EsqueIOInvalidPipelineBuilderState(
                "Errors while building pipeline object:\n" + "\n".join(self._errors)
            )

        return Pipeline(message_reader, message_writer, self._stream_decorators)

    def _build_message_reader(self) -> Optional[MessageReader]:
        if not self._input_state.is_valid():
            self._handle_invalid_input_state()
            return

        if self._input_state == _BuilderComponentState.NOTHING_DEFINED:
            message_reader = self._build_default_message_reader()

        elif self._input_state == _BuilderComponentState.READER_WRITER_DEFINED:
            message_reader = self._message_reader

        elif self._input_state == _BuilderComponentState.HANDLER_SERIALIZER_DEFINED:
            message_reader = HandlerSerializerMessageReader(self._input_handler, self._input_serializer)

        elif self._input_state == _BuilderComponentState.URI_DEFINED:
            message_reader = self._build_message_reader_from_uri()

        else:
            raise RuntimeError(
                "This shouldn't happen. We have a valid input state but no way of creating the message reader for it."
            )

        if self._start is not None:
            message_reader.seek(self._start)
        return message_reader

    def _build_default_message_reader(self) -> MessageReader:
        return HandlerSerializerMessageReader(
            self._create_default_input_handler(), self._create_default_input_serializer()
        )

    def _build_message_reader_from_uri(self) -> MessageReader:
        uri_config = UriConfig(self._input_uri)
        return HandlerSerializerMessageReader(
            handler=create_handler(uri_config.handler_config),
            message_serializer=MessageSerializer(
                key_serializer=create_serializer(uri_config.key_serializer_config),
                value_serializer=create_serializer(uri_config.value_serializer_config),
            ),
        )

    def _handle_invalid_input_state(self) -> None:
        if self._input_state == _BuilderComponentState.SERIALIZER_DEFINED:
            self._errors.append("Only input serializer was provided, need to also give an input handler.")
            return
        if self._input_state == _BuilderComponentState.HANDLER_DEFINED:
            self._errors.append("Only input handler was provided, need to also give an input serializer.")
            return

        self._errors.append("Ambiguous input state. Make sure not to provide more than one of the following.")
        if _BuilderComponentState.READER_WRITER_DEFINED in self._input_state:
            self._errors.append("Input reader was supplied.")
        if _BuilderComponentState.URI_DEFINED in self._input_state:
            self._errors.append("Input uri was supplied.")
        if _BuilderComponentState.HANDLER_SERIALIZER_DEFINED in self._input_state:
            self._errors.append("Input serializer and handler were supplied.")

    def _create_default_input_handler(self) -> BaseHandler:
        return PipeHandler(PipeHandlerConfig(host="stdin", path="", scheme="pipe"))

    def _create_default_input_serializer(self) -> MessageSerializer:
        serializer = StringSerializer(config=StringSerializerConfig(scheme="str"))
        return MessageSerializer(key_serializer=serializer, value_serializer=serializer)

    def _build_message_writer(self) -> Optional[MessageWriter]:
        if not self._output_state.is_valid():
            self._handle_invalid_output_state()
            return

        if self._output_state == _BuilderComponentState.NOTHING_DEFINED:
            return self._build_default_message_writer()

        if self._output_state == _BuilderComponentState.READER_WRITER_DEFINED:
            return self._message_writer

        if self._output_state == _BuilderComponentState.HANDLER_SERIALIZER_DEFINED:
            return HandlerSerializerMessageWriter(self._output_handler, self._output_serializer)

        if self._output_state == _BuilderComponentState.URI_DEFINED:
            return self._build_message_writer_from_uri()

    def _build_default_message_writer(self) -> MessageWriter:
        return HandlerSerializerMessageWriter(
            self._create_default_output_handler(), self._create_default_output_serializer()
        )

    def _build_message_writer_from_uri(self) -> MessageWriter:
        uri_config = UriConfig(self._output_uri)
        return HandlerSerializerMessageWriter(
            handler=create_handler(uri_config.handler_config),
            message_serializer=MessageSerializer(
                key_serializer=create_serializer(uri_config.key_serializer_config),
                value_serializer=create_serializer(uri_config.value_serializer_config),
            ),
        )

    def _handle_invalid_output_state(self) -> None:
        if self._output_state == _BuilderComponentState.SERIALIZER_DEFINED:
            self._errors.append("Only output serializer was provided, need to also give an output handler.")
            return
        if self._output_state == _BuilderComponentState.HANDLER_DEFINED:
            self._errors.append("Only output handler was provided, need to also give an output serializer.")
            return

        self._errors.append("Ambiguous output state. Make sure not to provide more than one of the following.")
        if _BuilderComponentState.READER_WRITER_DEFINED in self._output_state:
            self._errors.append("Output writer was supplied.")
        if _BuilderComponentState.URI_DEFINED in self._output_state:
            self._errors.append("Output uri was supplied.")
        if _BuilderComponentState.HANDLER_SERIALIZER_DEFINED in self._output_state:
            self._errors.append("Output serializer and handler were supplied.")

    def _create_default_output_handler(self) -> BaseHandler:
        return PipeHandler(PipeHandlerConfig(host="stdout", path="", scheme="pipe"))

    _create_default_output_serializer = _create_default_input_serializer

    def with_range(self, start: Optional[int] = None, limit: Optional[int] = None):
        self._start = start
        self._limit = limit

    def _add_limit_decorator(self):
        if self._limit is not None:
            self.with_stream_decorator(stop_after_nth_message(self._limit))

    def build(self) -> Pipeline:
        return self._pipeline
