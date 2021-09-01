import enum
import functools
from abc import ABC, abstractmethod
from typing import Callable, Iterable, List, Optional, Union

from esque.io.exceptions import EsqueIOInvalidPipelineBuilderState
from esque.io.handlers import BaseHandler, PipeHandler
from esque.io.handlers.pipe import PipeHandlerConfig
from esque.io.messages import Message
from esque.io.serializers import StringSerializer
from esque.io.serializers.base import MessageSerializer
from esque.io.serializers.string import StringSerializerConfig
from esque.io.stream_events import StreamEvent


class MessageReader(ABC):
    @abstractmethod
    def read_message(self) -> Message:
        raise NotImplementedError

    @abstractmethod
    def message_stream(self) -> Iterable[Message]:
        raise NotImplementedError

    @classmethod
    def from_uri(cls, uri: str) -> "MessageReader":
        pass
        # TODO continue here
        # parse_result: ParseResult = urllib.parse.urlparse(url)
        # url_schemes: List[str] = parse_result.scheme.split("+")
        # host: str = parse_result.netloc
        # path: str = parse_result.path
        # params: Dict[str, List[str]] = urllib.parse.parse_qs(parse_result.query)
        # handler_config_dict: Dict[str, Any] = {"host": host, "path": path, "scheme": url_schemes[0]}
        # handler_config_dict.update(extract_parameters("handler", params))
        # handler: BaseHandler = create_handler(handler_config_dict)
        # if len(url_schemes) == 1:
        #     key_serializer_config, value_serializer_config = handler.get_serializer_configs()
        # else:
        #     try:
        #         key_serializer_config, value_serializer_config = handler.get_serializer_configs()
        #     except EsqueIOSerializerConfigNotSupported:
        #         key_serializer_config = {}
        #         value_serializer_config = {}
        #     if len(url_schemes) == 2:
        #         key_serializer_config["scheme"] = url_schemes[1]
        #         value_serializer_config["scheme"] = url_schemes[1]
        #     else:
        #         key_serializer_config["scheme"] = url_schemes[1]
        #         value_serializer_config["scheme"] = url_schemes[2]
        # key_serializer_config.update(extract_parameters("key", params))
        # value_serializer_config.update(extract_parameters("value", params))
        # key_serializer = create_serializer(key_serializer_config)
        # value_serializer = create_serializer(value_serializer_config)
        # message_serializer: MessageSerializer = MessageSerializer(
        #     key_serializer=key_serializer, value_serializer=value_serializer
        # )
        # return cls(handler, message_serializer)


class MessageWriter(ABC):
    @abstractmethod
    def write_message(self, message: Message):
        raise NotImplementedError

    @abstractmethod
    def write_many_messages(self, messages: Iterable[Message]):
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
        return self._message_serializer.deserialize_many(binary_message_stream=self._handler.message_stream())


class HandlerSerializerMessageWriter(MessageWriter):
    _handler: BaseHandler
    _message_serializer: MessageSerializer

    def __init__(self, handler: BaseHandler, message_serializer: MessageSerializer):
        self._handler = handler
        self._message_serializer = message_serializer

    def write_message(self, message: Message):
        self._handler.write_message(binary_message=self._message_serializer.serialize(message=message))

    def write_many_messages(self, messages: Iterable[Message]):
        self._handler.write_many_messages(binary_messages=self._message_serializer.serialize_many(messages=messages))


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
        self._output_element.write_many_messages(self.decorated_message_stream())

    def write_message(self, message: Message):
        self._output_element.write_message(message=message)

    def write_many_messages(self, messages: Iterable[Message]):
        self._output_element.write_many_messages(messages=messages)

    # def do_the_work:
    # handle input
    # deserialize input
    # for transformation in transformations:
    #   messages = transformation.transform(messages)
    # serialize output
    # handle output


class BuilderComponentState(enum.IntFlag):
    NOTHING_DEFINED = 0
    URI_DEFINED = 1
    SERIALIZER_DEFINED = 2
    HANDLER_DEFINED = 4
    READER_WRITER_DEFINED = 8
    HANDLER_SERIALIZER_DEFINED = 6

    def is_valid(self) -> bool:
        return self in {
            BuilderComponentState.NOTHING_DEFINED,
            BuilderComponentState.URI_DEFINED,
            BuilderComponentState.READER_WRITER_DEFINED,
            BuilderComponentState.HANDLER_SERIALIZER_DEFINED,
        }


class PipelineBuilder:
    _input_handler: Optional[BaseHandler] = None
    _input_serializer: Optional[MessageSerializer] = None
    _message_reader: Optional[MessageReader] = None
    _input_uri: Optional[str] = None
    _input_state: BuilderComponentState = BuilderComponentState.NOTHING_DEFINED

    _output_handler: Optional[BaseHandler] = None
    _output_serializer: Optional[MessageSerializer] = None
    _message_writer: Optional[MessageWriter] = None
    _output_uri: Optional[str] = None
    _output_state: BuilderComponentState = BuilderComponentState.NOTHING_DEFINED

    _stream_decorators: List[Callable[[Iterable], Iterable]]
    _errors: List[str]

    def __init__(self):
        """
        Creates a new PipelineBuilder. In case no methods are called other than :meth:`PipelineBuilder.build()`, the created pipeline
        will have a pair of console handlers (stdin and stdout) and UTF-8 string message serializers.
        """
        self._stream_decorators = []
        self._errors = []

    def with_input_handler(self, handler: BaseHandler) -> "PipelineBuilder":
        if handler is not None:
            self._input_handler = handler
            self._input_state |= BuilderComponentState.HANDLER_DEFINED
        return self

    def with_input_message_serializer(self, serializer: MessageSerializer) -> "PipelineBuilder":
        if serializer is not None:
            self._input_serializer = serializer
            self._input_state |= BuilderComponentState.SERIALIZER_DEFINED
        return self

    def with_message_reader(self, message_reader: MessageReader) -> "PipelineBuilder":
        if message_reader is not None:
            self._message_reader = message_reader
            self._input_state |= BuilderComponentState.READER_WRITER_DEFINED
        return self

    def with_output_handler(self, handler: BaseHandler) -> "PipelineBuilder":
        if handler is not None:
            self._output_handler = handler
            self._output_state |= BuilderComponentState.HANDLER_DEFINED
        return self

    def with_output_message_serializer(self, serializer: MessageSerializer) -> "PipelineBuilder":
        if serializer is not None:
            self._output_serializer = serializer
            self._output_state |= BuilderComponentState.SERIALIZER_DEFINED
        return self

    def with_message_writer(self, message_writer: MessageWriter) -> "PipelineBuilder":
        if message_writer is not None:
            self._message_writer = message_writer
            self._output_state |= BuilderComponentState.READER_WRITER_DEFINED
        return self

    def with_stream_decorator(self, decorator: Callable[[Iterable], Iterable]) -> "PipelineBuilder":
        if decorator is not None:
            self._stream_decorators.append(decorator)
        return self

    def with_input_from_uri(self, uri: str) -> "PipelineBuilder":
        if uri is not None:
            self._input_uri = uri
            self._input_state |= BuilderComponentState.URI_DEFINED
        return self

    def with_output_from_uri(self, uri: str) -> "PipelineBuilder":
        if uri is not None:
            self._output_uri = uri
            self._output_state |= BuilderComponentState.URI_DEFINED
        return self

    def add_transformation(self, transformation) -> "PipelineBuilder":
        raise NotImplementedError

    @functools.cached_property
    def _pipeline(self) -> Pipeline:
        message_reader = self._build_message_reader()
        message_writer = self._build_message_writer()

        if self._errors:
            raise EsqueIOInvalidPipelineBuilderState(
                "Errors while building pipeline object:\n" + "\n".join(self._errors)
            )

        return Pipeline(message_reader, message_writer, self._stream_decorators)

    def _build_message_reader(self) -> Optional[MessageReader]:
        if not self._input_state.is_valid():
            self._handle_invalid_input_state()
            return

        if self._input_state == BuilderComponentState.NOTHING_DEFINED:
            return self._build_default_message_reader()

        if self._input_state == BuilderComponentState.READER_WRITER_DEFINED:
            return self._message_reader

        if self._input_state == BuilderComponentState.HANDLER_SERIALIZER_DEFINED:
            return HandlerSerializerMessageReader(self._input_handler, self._input_serializer)

        if self._input_state == BuilderComponentState.URI_DEFINED:
            return MessageReader.from_uri(self._input_uri)

    def _build_default_message_reader(self) -> MessageReader:
        return HandlerSerializerMessageReader(
            self._create_default_input_handler(), self._create_default_input_serializer()
        )

    def _handle_invalid_input_state(self) -> None:
        if self._input_state == BuilderComponentState.SERIALIZER_DEFINED:
            self._errors.append("Only input serializer was provided, need to also give an input handler.")
            return
        if self._input_state == BuilderComponentState.HANDLER_DEFINED:
            self._errors.append("Only input handler was provided, need to also give an input serializer.")
            return

        self._errors.append("Ambiguous input state. Make sure not to provide more than one of the following.")
        if BuilderComponentState.READER_WRITER_DEFINED in self._input_state:
            self._errors.append("Input reader was supplied.")
        if BuilderComponentState.URI_DEFINED in self._input_state:
            self._errors.append("Input uri was supplied.")
        if BuilderComponentState.HANDLER_SERIALIZER_DEFINED in self._input_state:
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

        if self._output_state == BuilderComponentState.NOTHING_DEFINED:
            return self._build_default_message_writer()

        if self._output_state == BuilderComponentState.READER_WRITER_DEFINED:
            return self._message_writer

        if self._output_state == BuilderComponentState.HANDLER_SERIALIZER_DEFINED:
            return HandlerSerializerMessageWriter(self._output_handler, self._output_serializer)

        if self._output_state == BuilderComponentState.URI_DEFINED:
            return MessageWriter.from_uri(self._output_uri)

    def _build_default_message_writer(self) -> MessageWriter:
        return HandlerSerializerMessageWriter(
            self._create_default_output_handler(), self._create_default_output_serializer()
        )

    def _handle_invalid_output_state(self) -> None:
        if self._output_state == BuilderComponentState.SERIALIZER_DEFINED:
            self._errors.append("Only output serializer was provided, need to also give an output handler.")
            return
        if self._output_state == BuilderComponentState.HANDLER_DEFINED:
            self._errors.append("Only output handler was provided, need to also give an output serializer.")
            return

        self._errors.append("Ambiguous output state. Make sure not to provide more than one of the following.")
        if BuilderComponentState.READER_WRITER_DEFINED in self._output_state:
            self._errors.append("Output writer was supplied.")
        if BuilderComponentState.URI_DEFINED in self._output_state:
            self._errors.append("Output uri was supplied.")
        if BuilderComponentState.HANDLER_SERIALIZER_DEFINED in self._output_state:
            self._errors.append("Output serializer and handler were supplied.")

    def _create_default_output_handler(self) -> BaseHandler:
        return PipeHandler(PipeHandlerConfig(host="stdout", path="", scheme="pipe"))

    _create_default_output_serializer = _create_default_input_serializer

    def build(self) -> Pipeline:
        return self._pipeline
