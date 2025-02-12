from pathlib import Path
from unittest import mock

import pytest
import yaml
from pytest_cases import fixture
from yaml.scanner import ScannerError

from esque.config import Config
from esque.config.migration import CURRENT_VERSION, migrate
from esque.errors import ConfigTooNew, ConfigTooOld, ContextNotDefinedException
from esque.validation import validate_esque_config
from tests.conftest import config_loader


@fixture(params=[pytest.param(version, id=f"v{version}") for version in range(0, CURRENT_VERSION + 1)])
def config_version(request) -> int:
    return request.param


@fixture
def config(config_version: int, load_config: config_loader):
    old_conf, _ = load_config(config_version)
    new_path, _ = migrate(Path(old_conf))
    Config.set_instance(Config())
    return Config.get_instance()


def test_backup(config_version: int, load_config: config_loader):
    old_conf, old_yaml = load_config(config_version)
    _, backup = migrate(Path(old_conf))
    if config_version == CURRENT_VERSION:
        assert backup is None, "No need for backup"
        return

    assert backup.read_text() == old_yaml


def test_schema(config_version: int, load_config: config_loader):
    old_conf, _ = load_config(config_version)
    new_path, _ = migrate(Path(old_conf))

    validate_esque_config(yaml.safe_load(new_path.read_text()))


def test_available_contexts(config: Config):
    assert config.available_contexts == [f"context_{i}" for i in range(1, 6)]


def test_current_context(config: Config):
    assert config.current_context == "context_1"


def test_current_context_dict(config: Config):
    expected = {"bootstrap_servers": ["localhost:9091"], "security_protocol": "PLAINTEXT"}
    assert config.current_context_dict == expected


def test_context_switch(config: Config):
    assert config.current_context == "context_1"
    config.context_switch("context_2")
    assert config.current_context == "context_2"


def test_context_switch_to_not_existing_context_fails(config: Config):
    assert config.current_context == "context_1"
    with pytest.raises(ContextNotDefinedException):
        config.context_switch("bla")


def test_current_context_bootstrap_servers(config: Config):
    assert config.bootstrap_servers == ["localhost:9091"]

    config.context_switch("context_3")

    assert config.bootstrap_servers == [
        "node01.cool-domain.com:9093",
        "node02.cool-domain.com:9093",
        "node03.cool-domain.com:9093",
    ]


def test_current_context_schema_registry(config: Config):
    with pytest.raises(KeyError):
        _ = config.schema_registry
    config.context_switch("context_5")
    assert config.schema_registry == "http://schema-registry.example.com"


def test_ssl_params(config: Config):
    assert config.ssl_params == {}
    config.context_switch("context_5")
    assert config.ssl_params == {
        "cafile": "/my/ca.crt",
        "certfile": "/my/certificate.crt",
        "keyfile": "/my/certificate.key",
        "password": "mySecretPassword",
    }


def test_sasl_params(config: Config):
    assert config.sasl_params == {}
    config.context_switch("context_5")
    assert config.sasl_params == {"mechanism": "PLAIN", "user": "alice", "password": "alice-secret"}
    assert config.sasl_mechanism == "PLAIN"


def test_kafka_python_config(config: Config):
    config.context_switch("context_5")
    expected_config = {
        "bootstrap_servers": ["kafka:9094", "kafka1:9094", "kafka2:9094", "kafka3:9094"],
        "security_protocol": "SASL_SSL",
        "sasl_mechanism": "PLAIN",
        "sasl_plain_username": "alice",
        "sasl_plain_password": "alice-secret",
        "ssl_cafile": "/my/ca.crt",
        "ssl_certfile": "/my/certificate.crt",
        "ssl_keyfile": "/my/certificate.key",
        "ssl_password": "mySecretPassword",
    }

    actual_config = config.create_kafka_python_config()
    assert expected_config == actual_config


def test_confluent_config(config: Config):
    config.context_switch("context_5")
    expected_config = {
        "bootstrap.servers": "kafka:9094,kafka1:9094,kafka2:9094,kafka3:9094",
        "security.protocol": "SASL_SSL",
        "schema.registry.url": "http://schema-registry.example.com",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": "alice",
        "sasl.password": "alice-secret",
        "ssl.ca.location": "/my/ca.crt",
        "ssl.certificate.location": "/my/certificate.crt",
        "ssl.key.location": "/my/certificate.key",
        "ssl.key.password": "mySecretPassword",
    }

    actual_config = config.create_confluent_config(include_schema_registry=True)
    assert expected_config == actual_config


def test_default_values(config: Config):
    config.context_switch("context_3")
    assert config.default_num_partitions == 2
    assert config.default_replication_factor == 2


def test_default_values_not_specified(mocker: mock, config: Config):
    assert config.default_values == {}
    assert config.default_num_partitions == 1

    # if not given, broker defaults are taken
    setting_mock = mocker.patch.object(config, "_get_broker_setting", return_value="1234")
    assert config.default_replication_factor == 1234
    setting_mock.assert_called_with("default.replication.factor")
    assert config.default_num_partitions == 1234
    setting_mock.assert_called_with("num.partitions")


def test_config_too_old(load_config: config_loader):
    conf_path, _ = load_config(CURRENT_VERSION - 1)

    with pytest.raises(ConfigTooOld):
        Config()


def test_config_too_new(load_config: config_loader):
    conf_path, conf_content = load_config()
    data = yaml.safe_load(conf_content)
    data["version"] = CURRENT_VERSION + 1
    with conf_path.open("w") as f:
        yaml.dump(data, f)

    with pytest.raises(ConfigTooNew):
        Config()


def test_invalid_config(load_config: config_loader):
    conf_path, conf_content = load_config()
    conf_content += '\nasdf:"'
    conf_path.write_text(conf_content)

    with pytest.raises(ScannerError):
        Config()


def test_validation_called(mocker: mock, load_config: config_loader):
    conf_path, conf_content = load_config()
    validator_mock = mocker.patch("esque.validation.validate_esque_config")
    Config()

    (validated_config_dict,) = validator_mock.call_args[0]
    assert validated_config_dict == yaml.safe_load(conf_content)
