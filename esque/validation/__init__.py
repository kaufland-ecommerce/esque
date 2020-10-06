from pathlib import Path
from typing import Dict

import yamale

from esque.errors import ValidationException, YamaleValidationException
from esque.validation import yamale_validators

SCHEMA_DIR = Path(__file__).resolve().parent / "schemas"


def validate_topic_config(topic_config: Dict) -> None:
    validate(topic_config, SCHEMA_DIR / "topic.yaml")


def validate_editable_topic_config(editable_topic_config: Dict) -> None:
    validate(editable_topic_config, SCHEMA_DIR / "editable_topic.yaml")


def validate_offset_config(offset_config: Dict) -> None:
    validate(offset_config, SCHEMA_DIR / "offset_config.yaml")


def validate_esque_config(esque_config: Dict) -> None:
    validate(esque_config, SCHEMA_DIR / "esque_config.yaml")
    ctx = esque_config["current_context"]
    all_ctx = sorted(esque_config["contexts"])
    if ctx not in all_ctx:
        raise ValidationException(f"Context {ctx} does not exist. Available contexts {all_ctx}")


def validate(data: Dict, schema: Path) -> None:
    schema = yamale.make_schema(schema, validators=yamale_validators.all_validators())
    try:
        yamale.validate(schema, [(data, "<dict literal>")], strict=True)
    except ValueError as validation_error:
        raise YamaleValidationException(validation_error)
