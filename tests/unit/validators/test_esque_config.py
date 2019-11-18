import typing

import pytest
import yaml

import esque.errors
from esque.validation import validate_esque_config
from tests.conftest import LOAD_SAMPLE_CONFIG, get_path_for_config_version


@pytest.fixture
def sample_config_data() -> typing.Dict:
    return yaml.safe_load(get_path_for_config_version(LOAD_SAMPLE_CONFIG).read_text())


def get_context(config: typing.Dict, context: typing.Optional[str] = None) -> typing.Dict:
    if context is None:
        context = config["current_context"]
    return config["contexts"][context]


def test_sample_config_valid(sample_config_data: typing.Dict):
    validate_esque_config(sample_config_data)


def test_mixed_case_security_protocol(sample_config_data: typing.Dict):
    get_context(sample_config_data)["security_protocol"] = "sasL_PLaintEXT"
    validate_esque_config(sample_config_data)


def test_invalid_security_protocol(sample_config_data: typing.Dict):
    get_context(sample_config_data)["security_protocol"] = "sasLaintEXT"
    with pytest.raises(esque.errors.ValidationException):
        validate_esque_config(sample_config_data)


def test_mixed_case_sasl_mechanism(sample_config_data: typing.Dict):
    get_context(sample_config_data, "sasl_enabled")["sasl_params"]["mechanism"] = "SCRAM-sha-512"
    validate_esque_config(sample_config_data)


def test_invalid_sasl_mechanism(sample_config_data: typing.Dict):
    get_context(sample_config_data, "sasl_enabled")["sasl_params"]["mechanism"] = "SCRAM-sh-512"
    with pytest.raises(esque.errors.ValidationException):
        validate_esque_config(sample_config_data)


def test_non_existing_context(sample_config_data: typing.Dict):
    sample_config_data["current_context"] = "foo_bar_baz"
    with pytest.raises(esque.errors.ValidationException):
        validate_esque_config(sample_config_data)


def test_invalid_url(sample_config_data: typing.Dict):
    get_context(sample_config_data)["schema_registry"] = "http://10..15.10.10:8081"

    with pytest.raises(esque.errors.ValidationException):
        validate_esque_config(sample_config_data)
