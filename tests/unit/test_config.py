from functools import wraps
from pathlib import Path
from typing import Callable

import pytest
from esque.config import Config
from esque.config.migration import migrate
from esque.errors import ContextNotDefinedException


@pytest.fixture(params=[
    pytest.param(0, id="v0"),
    pytest.param(1, id="v1"),
])
def config_version(request) -> int:
    return request.param


@pytest.fixture
def config(config_version, load_config, mock_config_path):
    old_conf, _ = load_config(config_version)
    new_path = migrate(Path(old_conf))
    mock_config_path(new_path)
    return Config()


def test_available_contexts(config: Config):
    assert config.available_contexts == [f"context_{i}" for i in range(1, 5)]


def test_current_context(config: Config):
    assert config.current_context == "context_1"


def test_current_context_dict(config: Config):
    expected = {"bootstrap_hosts": "localhost", "bootstrap_port": "9091", "security_protocol": "PLAINTEXT"}
    assert config.current_context_dict == expected


def test_current_context_port(config: Config):
    assert config.bootstrap_port == "9091"


def test_context_switch(config: Config):
    assert config.current_context == "context_1"
    config.context_switch("context_2")
    assert config.current_context == "context_2"


def test_context_switch_to_not_existing_context_fails(config: Config):
    assert config.current_context == "context_1"
    with pytest.raises(ContextNotDefinedException):
        config.context_switch("bla")


def test_current_context_hosts(config: Config):
    assert config.bootstrap_hosts == ["localhost"]

    config.context_switch("context_3")

    assert config.bootstrap_hosts == ["node01", "node02", "node03"]


def test_current_context_bootstrap_servers(config: Config):
    assert config.bootstrap_servers == ["localhost:9091"]

    config.context_switch("context_3")

    assert config.bootstrap_servers == [
        "node01.cool-domain.com:9093",
        "node02.cool-domain.com:9093",
        "node03.cool-domain.com:9093",
    ]


def test_current_context_bootstrap_domain(config: Config):
    assert config.bootstrap_domain is None

    config.context_switch("context_2")

    assert config.bootstrap_domain == "dummy_domain"
