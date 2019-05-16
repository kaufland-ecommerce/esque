from pathlib import Path

import pytest
from esque.config import Config
from esque.errors import ContextNotDefinedException

DUMMY_CONFIG = """
[Context]
current = context_1

[Context.context_1]
bootstrap_hosts = localhost
bootstrap_port = 9091
security_protocol = PLAINTEXT

[Context.context_2]
bootstrap_hosts = broker01,broker02,broker03
bootstrap_domain = dummy_domain
bootstrap_port = 9092
security_protocol = PLAINTEXT

[Context.context_3]
bootstrap_hosts = node01,node02,node03
bootstrap_port = 9093
bootstrap_domain = cool-domain.com
security_protocol = PLAINTEXT

[Context.context_4]
bootstrap_hosts = kafka
bootstrap_port = 9094
security_protocol = PLAINTEXT
"""


@pytest.fixture()
def dummy_config(mocker, tmpdir_factory):
    fn: Path = tmpdir_factory.mktemp("config").join("dummy.cfg")
    fn.write_text(DUMMY_CONFIG, encoding="UTF-8")
    mocker.patch("esque.config.config_path", return_value=fn)
    yield fn


@pytest.fixture()
def config(dummy_config):
    config = Config()
    yield config


def test_available_contexts(config: Config):
    assert config.available_contexts == [f"context_{i}" for i in range(1, 5)]


def test_current_context(config: Config):
    assert config.current_context == "context_1"


def test_current_context_dict(config: Config):
    expected = {
        "bootstrap_hosts": "localhost",
        "bootstrap_port": "9091",
        "security_protocol": "PLAINTEXT",
    }
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
