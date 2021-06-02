from typing import Any, Dict, Type

from esque.io.exceptions import EsqueIOSerializerConfigException
from esque.io.serializers.avro import AvroSerializer
from esque.io.serializers.base import BaseSerializer, SerializerConfig
from esque.io.serializers.json import JsonSerializer
from esque.io.serializers.string import StringSerializer

SERIALIZER_LOOKUP: Dict[str, Type[BaseSerializer]] = {
    "str": StringSerializer,
    "avro": AvroSerializer,
    "json": JsonSerializer,
}


def create_serializer(serializer_config_dict: Dict[str, Any]) -> BaseSerializer:
    serializer_cls: Type[BaseSerializer] = SERIALIZER_LOOKUP.get(serializer_config_dict.get("scheme"))
    if serializer_cls is None:
        raise EsqueIOSerializerConfigException(
            f"Unrecognized serializer identifier: {serializer_config_dict.get('scheme')}. "
            f"Possible values {', '.join(SERIALIZER_LOOKUP.keys())}"
        )
    serializer_config: SerializerConfig = serializer_cls.config_cls(**serializer_config_dict)
    return serializer_cls(config=serializer_config)
