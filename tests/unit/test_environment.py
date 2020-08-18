import pytest

from esque.cli import environment
from esque.config import Config
from esque.errors import YamaleValidationException


def test_environment_loading(os_environ_variables):
    verbose = environment.ESQUE_VERBOSE
    assert verbose == os_environ_variables["ESQUE_VERBOSE"]


def test_environment_types(monkeypatch):
    monkeypatch.setenv("ESQUE_CONTEXT_ENABLED", "False")
    assert environment.ESQUE_CONTEXT_ENABLED is False

    monkeypatch.setenv("ESQUE_CONTEXT_ENABLED", "True")
    assert environment.ESQUE_CONTEXT_ENABLED is True

    assert environment.ESQUE_SCHEMA_REGISTRY is None

    monkeypatch.setenv("ESQUE_SCHEMA_REGISTRY", "localhost:8081")
    assert environment.ESQUE_SCHEMA_REGISTRY == "localhost:8081"

    monkeypatch.setenv("ESQUE_BOOTSTRAP_SERVERS", "broker1,broker2")
    assert environment.ESQUE_BOOTSTRAP_SERVERS == ["broker1", "broker2"]


def test_undefined_env_variable_fails():
    with pytest.raises(AttributeError):
        _ = environment.RIDICULOUS_ENV_VARIABLE


def test_env_config_fails(unittest_config: Config):
    with pytest.raises(YamaleValidationException):
        _ = unittest_config.env_context


# TODO: Currently does not work. Fix this. Apparently Config doesn't get patched envs here.
# def test_proper_config(monkeypatch):
#     conf = {
#         "ESQUE_CONTEXT_ENABLED": "True",
#         "ESQUE_BOOTSTRAP_SERVERS": "broker1:9092,broker2:9092",
#         "ESQUE_SECURITY_PROTOCOL": "PLAINTEXT",
#         "ESQUE_SCHEMA_REGISTRY": "localhost:8082",
#         "ESQUE_NUM_PARTITIONS": "1",
#         "ESQUE_REPLICATION_FACTOR": "1",
#     }
#
#     expected_context = {
#         "bootstrap_servers": ["broker1:9092", "broker2:9092"],
#         "security_protocol": "PLAINTEXT",
#         "schema_registry": "localhost:8082",
#         "default_values": {"num_partitions": 1, "replication_factor": 1},
#     }
#
#     assert "PLAINTEXT" == environment.ESQUE_SECURITY_PROTOCOL
#
#     for k, v in conf.items():
#         monkeypatch.setenv(k, v)
#
#     Config.set_instance(None)
#     context = Config().env_context
#
#     assert context == expected_context
