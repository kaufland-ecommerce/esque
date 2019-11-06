from pathlib import Path
from typing import Callable
from unittest import mock

import pytest
import yaml
from yaml.scanner import ScannerError

from esque import validation
from esque.config import Config
from esque.config.migration import CURRENT_VERSION, migrate
from esque.errors import ConfigTooNew, ConfigTooOld, ContextNotDefinedException, EsqueConfigNotValidException
from esque.validation import validate_esque_config
from tests.conftest import config_loader, config_path_mocker


@pytest.fixture(params=[pytest.param(version, id=f"v{version}") for version in range(0, CURRENT_VERSION + 1)])
def config_version(request) -> int:
    return request.param


@pytest.fixture
def config(config_version: int, load_config: config_loader, mock_config_path: config_path_mocker):
    old_conf, _ = load_config(config_version)
    new_path, _ = migrate(Path(old_conf))
    mock_config_path(new_path)
    return Config()


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


def test_ssl_params(config: Config):
    assert config.ssl_params == {}
    config.context_switch("context_5")
    assert config.ssl_params == {
        "cafile": "/my/ca.crt",
        "certfile": "/my/certificate.crt",
        "keyfile": "/my/certificate.key",
    }


def test_sasl_params(config: Config):
    assert config.sasl_params == {}
    config.context_switch("context_5")
    assert config.sasl_params == {"mechanism": "PLAIN", "user": "alice", "password": "alice-secret"}


def test_default_values(config: Config):
    config.context_switch("context_3")
    assert config.default_num_partitions == 2
    assert config.default_replication_factor == 2


def test_default_values_not_specified(config: Config):
    assert config.default_values == {}
    assert config.default_num_partitions == 1

    # if not given, default replication factor is min(len(bootstrap_servers),3)
    assert len(config.bootstrap_servers) == 1
    assert config.default_replication_factor == 1

    config.context_switch("context_5")
    assert config.default_values == {}
    assert len(config.bootstrap_servers) == 4
    assert config.default_replication_factor == 3
    assert config.default_num_partitions == 1


def test_config_too_old(mock_config_path: config_path_mocker, load_config: config_loader):
    conf_path, _ = load_config(CURRENT_VERSION - 1)
    mock_config_path(conf_path)

    with pytest.raises(ConfigTooOld):
        Config()


def test_config_too_new(mock_config_path: config_path_mocker, load_config: config_loader):
    conf_path, conf_content = load_config()
    data = yaml.safe_load(conf_content)
    data["version"] = CURRENT_VERSION + 1
    with conf_path.open("w") as f:
        yaml.dump(data, f)
    mock_config_path(conf_path)

    with pytest.raises(ConfigTooNew):
        Config()


def test_invalid_config(mock_config_path: config_path_mocker, load_config: config_loader):
    conf_path, conf_content = load_config()
    conf_content += '\nasdf:"'
    conf_path.write_text(conf_content)
    mock_config_path(conf_path)

    with pytest.raises(ScannerError):
        Config()


def test_validation_called(mocker: mock, mock_config_path: config_path_mocker, load_config: config_loader):
    conf_path, conf_content = load_config()
    mock_config_path(conf_path)
    validator_mock = mocker.patch.object(validation, "validate")
    Config()

    validated_config_dict, schema_path, exc_type = validator_mock.call_args[0]
    assert schema_path.name == "esque_config.yaml"
    assert validated_config_dict == yaml.safe_load(conf_content)
    assert exc_type == EsqueConfigNotValidException
