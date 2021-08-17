import urllib.parse
from abc import ABC, abstractmethod
from typing import Any, Dict, Iterable, List
from urllib.parse import ParseResult

from esque.config import Config
from esque.io.exceptions import EsqueIOInvalidPipelineBuilderState, EsqueIOSerializerConfigNotSupported
from esque.io.handlers import BaseHandler, create_handler
from esque.io.helpers import extract_parameters
from esque.io.messages import Message
from esque.io.serializers import create_serializer
from esque.io.serializers.base import MessageSerializer


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
    _serializer: MessageSerializer

    def __init__(self, handler: BaseHandler, serializer: MessageSerializer):
        self._handler = handler
        self._serializer = serializer

    def read_message(self) -> Message:
        return self._serializer.deserialize(binary_message=self._handler.read_message())

    def read_many_messages(self) -> Iterable[Message]:
        return self._serializer.deserialize_many(binary_messages=self._handler.read_many_messages())


class HandlerSerializerMessageWriter(MessageWriter):
    _handler: BaseHandler
    _serializer: MessageSerializer

    def __init__(self, handler: BaseHandler, serializer: MessageSerializer):
        self._handler = handler
        self._serializer = serializer

    def write_message(self, message: Message):
        self._handler.write_message(binary_message=self._serializer.serialize(message=message))

    def write_many_messages(self, messages: Iterable[Message]):
        self._handler.write_many_messages(binary_messages=self._serializer.serialize_many(messages=messages))


class Pipeline:
    _input_element: MessageReader
    _output_element: MessageWriter

    # def do_the_work:
    # handle input
    # deserialize input
    # for transformation in transformations:
    #   messages = transformation.transform(messages)
    # serialize output
    # handle output


class PipelineBuilder:
    _pipeline: "Pipeline" = Pipeline()

    def __init__(self):
        pass

    def with_input_handler(self, handler: BaseHandler) -> "PipelineBuilder":
        raise NotImplementedError

    def with_input_message_serializer(self, serializer: MessageSerializer) -> "PipelineBuilder":
        raise NotImplementedError

    def with_output_handler(self, handler: BaseHandler) -> "PipelineBuilder":
        raise NotImplementedError

    def with_output_message_serializer(self, serializer: MessageSerializer) -> "PipelineBuilder":
        raise NotImplementedError

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
        raise EsqueIOInvalidPipelineBuilderState("Pipeline builder is missing one or more components")
