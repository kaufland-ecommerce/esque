from pathlib import Path
from typing import Dict

import yamale

from esque.errors import TopicConfigNotValidException
from . import validators

SCHEMA_DIR = Path(__file__).resolve().parent / "schemas"


def validate_topic_config(topic_config: Dict) -> None:
    validate(topic_config, SCHEMA_DIR / "topic.yaml")


def validate_editable_topic_config(editable_topic_config: Dict) -> None:
    validate(editable_topic_config, SCHEMA_DIR / "editable_topic.yaml")


def validate_esque_config(editable_topic_config: Dict) -> None:
    validate(editable_topic_config, SCHEMA_DIR / "esque_config.yaml")


def validate(data: Dict, schema: Path) -> None:
    schema = yamale.make_schema(schema, validators=validators.all_validators())
    try:
        yamale.validate(schema, [(data, "<dict literal>")], strict=True)
    except ValueError as validation_error:
        raise TopicConfigNotValidException(validation_error)
