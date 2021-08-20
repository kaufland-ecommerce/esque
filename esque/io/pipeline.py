from abc import ABC, abstractmethod
from typing import Iterable, Union

from esque.io.handlers import BaseHandler, PipeHandler
from esque.io.handlers.pipe import PipeHandlerConfig
from esque.io.messages import Message
from esque.io.serializers import StringSerializer
from esque.io.serializers.base import MessageSerializer
from esque.io.stream_events import StreamEvent


class MessageReader(ABC):
    @abstractmethod
    def read_message(self) -> Message:
        raise NotImplementedError

    @abstractmethod
    def read_many_messages(self) -> Iterable[Message]:
        raise NotImplementedError


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

    def read_many_messages(self) -> Iterable[Message]:
        return self._message_serializer.deserialize_many(binary_messages=self._handler.message_stream())


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

    def __init__(self, input_element: MessageReader, output_element: MessageWriter):
        self._input_element = input_element
        self._output_element = output_element

    def execute(self):
        self._output_element.write_many_messages(self._input_element.read_many_messages())

    # def do_the_work:
    # handle input
    # deserialize input
    # for transformation in transformations:
    #   messages = transformation.transform(messages)
    # serialize output
    # handle output


class PipelineBuilder:
    _pipeline: "Pipeline"
    _input_handler: BaseHandler = PipeHandler(PipeHandlerConfig(host="stdin", path="", scheme="pipe"))
    _input_serializer: MessageSerializer = MessageSerializer(StringSerializer())
    _output_handler: BaseHandler = PipeHandler(PipeHandlerConfig(host="stdout", path="", scheme="pipe"))
    _output_serializer: MessageSerializer = MessageSerializer(StringSerializer())

    def __init__(self):
        """
        Creates a new PipelineBuilder. In case no methods are called other than :meth:`PipelineBuilder.build()`, the created pipeline
        will have a pair of console handlers (stdin and stdout) and UTF-8 string message serializers.
        """
        pass

    def with_input_handler(self, handler: BaseHandler) -> "PipelineBuilder":
        if handler is not None:
            self._input_handler = handler
        return self

    def with_input_message_serializer(self, serializer: MessageSerializer) -> "PipelineBuilder":
        if serializer is not None:
            self._input_serializer = serializer
        return self

    def with_output_handler(self, handler: BaseHandler) -> "PipelineBuilder":
        if handler is not None:
            self._output_handler = handler
        return self

    def with_output_message_serializer(self, serializer: MessageSerializer) -> "PipelineBuilder":
        if serializer is not None:
            self._output_serializer = serializer
        return self

    def with_input_from_uri(self, uri: str) -> "PipelineBuilder":

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
        pass

    def add_transformation(self, transformation) -> "PipelineBuilder":
        raise NotImplementedError

    def build(self) -> Pipeline:
        if not self._pipeline:
            self._pipeline = Pipeline(
                HandlerSerializerMessageReader(handler=self._input_handler, message_serializer=self._input_serializer),
                HandlerSerializerMessageWriter(
                    handler=self._output_handler, message_serializer=self._output_serializer
                ),
            )
        return self._pipeline
