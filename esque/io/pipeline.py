import urllib.parse
from abc import ABC
from typing import Any, Dict, List, Type, TypeVar
from urllib.parse import ParseResult

from esque.io.exceptions import EsqueIOSerializerConfigNotSupported
from esque.io.handlers import BaseHandler, create_handler
from esque.io.helpers import extract_parameters
from esque.io.serializers import create_serializer
from esque.io.serializers.base import MessageSerializer

PE = TypeVar("PE", bound="PipelineElement")


class PipelineElement(ABC):
    def __init__(self, handler: BaseHandler, message_serializer: MessageSerializer):
        self._handler = handler
        self._message_serializer = message_serializer

    @classmethod
    def from_url(cls: Type[PE], url: str) -> PE:
        parse_result: ParseResult = urllib.parse.urlparse(url)
        url_schemes: List[str] = parse_result.scheme.split("+")
        host: str = parse_result.netloc
        path: str = parse_result.path
        params: Dict[str, List[str]] = urllib.parse.parse_qs(parse_result.query)
        handler_config_dict: Dict[str, Any] = {"host": host, "path": path, "scheme": url_schemes[0]}
        handler_config_dict.update(extract_parameters("handler", params))
        handler: BaseHandler = create_handler(handler_config_dict)
        if len(url_schemes) == 1:
            key_serializer_config, value_serializer_config = handler.get_serializer_configs()
        else:
            try:
                key_serializer_config, value_serializer_config = handler.get_serializer_configs()
            except EsqueIOSerializerConfigNotSupported:
                key_serializer_config = {}
                value_serializer_config = {}
            if len(url_schemes) == 2:
                key_serializer_config["scheme"] = url_schemes[1]
                value_serializer_config["scheme"] = url_schemes[1]
            else:
                key_serializer_config["scheme"] = url_schemes[1]
                value_serializer_config["scheme"] = url_schemes[2]
        key_serializer_config.update(extract_parameters("key", params))
        value_serializer_config.update(extract_parameters("value", params))
        key_serializer = create_serializer(key_serializer_config)
        value_serializer = create_serializer(value_serializer_config)
        message_serializer: MessageSerializer = MessageSerializer(
            key_serializer=key_serializer, value_serializer=value_serializer
        )
        return cls(handler, message_serializer)


class MessageWriter(PipelineElement):
    pass


class MessageReader(PipelineElement):
    pass


class MessageFilter:
    pass


class FilteredMessageReader(MessageReader):
    pass


class Pipeline:
    pass
