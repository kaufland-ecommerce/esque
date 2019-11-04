from pathlib import Path

import yamale

from esque.errors import TopicConfigNotValidException
from . import validators

SCHEMA_DIR = Path(__file__).resolve().parent / 'schemas'


def validate_topic_config(topic_config: Path) -> None:
    validate(topic_config, SCHEMA_DIR / 'topic.yaml')


def validate(file: Path, schema: Path) -> None:
    schema = yamale.make_schema(schema, validators=validators.all_validators())
    try:
        yamale.validate(schema, file, strict=True)
    except ValueError as validation_error:
        raise TopicConfigNotValidException(validation_error)