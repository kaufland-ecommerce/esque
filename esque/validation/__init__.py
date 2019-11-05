from pathlib import Path
from typing import Dict, Type, Optional

import yamale

from esque.errors import YamaleValidationException, TopicConfigNotValidException, EsqueConfigNotValidException
from . import validators

SCHEMA_DIR = Path(__file__).resolve().parent / "schemas"


def validate_topic_config(topic_config: Dict) -> None:
    validate(topic_config, SCHEMA_DIR / "topic.yaml", TopicConfigNotValidException)


def validate_editable_topic_config(editable_topic_config: Dict) -> None:
    validate(editable_topic_config, SCHEMA_DIR / "editable_topic.yaml", TopicConfigNotValidException)


def validate_esque_config(editable_topic_config: Dict) -> None:
    validate(editable_topic_config, SCHEMA_DIR / "esque_config.yaml", EsqueConfigNotValidException)


def validate(data: Dict, schema: Path, exc_type: Optional[Type[YamaleValidationException]] = None) -> None:
    if exc_type is None:
        exc_type = YamaleValidationException
    schema = yamale.make_schema(schema, validators=validators.all_validators())
    try:
        yamale.validate(schema, [(data, "<dict literal>")], strict=True)
    except ValueError as validation_error:
        raise exc_type(validation_error)
