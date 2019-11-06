from pathlib import Path
from typing import Callable

import pytest
import yaml

from esque.config import Config
from esque.config.migration import CURRENT_VERSION, migrate
from esque.errors import ContextNotDefinedException
from esque.validation import validate_esque_config
from tests.conftest import config_loader


@pytest.fixture(params=[pytest.param(version, id=f"v{version}") for version in range(0, CURRENT_VERSION + 1)])
def config_version(request) -> int:
    return request.param


@pytest.fixture
def config(config_version: int, load_config: config_loader, mock_config_path: Callable[[Path], None]):
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
