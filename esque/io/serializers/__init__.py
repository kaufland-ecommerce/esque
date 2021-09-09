from typing import Any, Dict, Type

from esque.io.exceptions import EsqueIOSerializerConfigException
from esque.io.serializers.base import DataSerializer, SerializerConfig
from esque.io.serializers.json import JsonSerializer
from esque.io.serializers.raw import RawSerializer
from esque.io.serializers.registry_avro import RegistryAvroSerializer
from esque.io.serializers.string import StringSerializer

# Make sure you only use valid characters here.
# Valid characters are a-z, A-Z, 0-9 and +-.
SERIALIZER_LOOKUP: Dict[str, Type[DataSerializer]] = {
    "str": StringSerializer,
    "reg-avro": RegistryAvroSerializer,
    "json": JsonSerializer,
    "raw": RawSerializer,
}


def create_serializer(serializer_config_dict: Dict[str, Any]) -> DataSerializer:
    serializer_cls: Type[DataSerializer] = SERIALIZER_LOOKUP.get(serializer_config_dict.get("scheme"))
    if serializer_cls is None:
        raise EsqueIOSerializerConfigException(
            f"Unrecognized serializer scheme: {serializer_config_dict.get('scheme')}. "
            f"Possible values {', '.join(SERIALIZER_LOOKUP.keys())}"
        )
    serializer_config: SerializerConfig = serializer_cls.config_cls(**serializer_config_dict)
    return serializer_cls(config=serializer_config)
