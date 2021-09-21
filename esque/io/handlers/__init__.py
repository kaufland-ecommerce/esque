from typing import Any, Dict, Type

from esque.io.exceptions import EsqueIOHandlerConfigException
from esque.io.handlers.base import BaseHandler, HandlerConfig
from esque.io.handlers.kafka import KafkaHandler
from esque.io.handlers.path import PathHandler
from esque.io.handlers.pipe import PipeHandler

HANDLER_LOOKUP: Dict[str, Type[BaseHandler]] = {"pipe": PipeHandler, "kafka": KafkaHandler, "path": PathHandler}


def create_handler(handler_config_dict: Dict[str, Any]) -> BaseHandler:
    handler_cls: Type[BaseHandler] = HANDLER_LOOKUP.get(handler_config_dict.get("scheme"))
    if handler_cls is None:
        raise EsqueIOHandlerConfigException(
            f"Unrecognized handler scheme: {handler_config_dict.get('scheme')}. "
            f"Possible values {', '.join(HANDLER_LOOKUP.keys())}"
        )
    handler_config: HandlerConfig = handler_cls.config_cls(**handler_config_dict)
    return handler_cls(config=handler_config)
