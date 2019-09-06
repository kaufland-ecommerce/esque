import configparser
import random
import string
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import click

from esque.environment import ESQUE_CONF_PATH
from esque.errors import ConfigNotExistsException, ContextNotDefinedException

RANDOM = "".join(random.choices(string.ascii_lowercase, k=8))

PING_TOPIC = f"ping-{RANDOM}"
PING_GROUP_ID = f"ping-{RANDOM}"

SLEEP_INTERVAL = 2


def config_dir() -> Path:
    return Path(click.get_app_dir("esque", force_posix=True))


def config_path() -> Path:
    if ESQUE_CONF_PATH:
        return Path(ESQUE_CONF_PATH)
    return config_dir() / "esque.cfg"


def sample_config_path() -> Path:
    return Path(__file__).parent / "config" / "sample_config.cfg"


class Config:
    def __init__(self):
        self._cfg = configparser.ConfigParser()
        if config_path().exists():
            self._cfg.read(config_path())
        else:
            raise ConfigNotExistsException("No config defined.")

    @property
    def available_contexts(self):
        return sorted([key.split(".")[1] for key in self._cfg.keys() if key.startswith("Context.")])

    @property
    def current_context(self):
        return self._cfg.get("Context", "current")

    @property
    def _current_section(self):
        return f"Context.{self._cfg.get('Context', 'current')}"

    @property
    def current_context_dict(self) -> Dict[str, Any]:
        return {
            option: self._cfg.get(self._current_section, option) for option in self._cfg.options(self._current_section)
        }

    @property
    def bootstrap_port(self) -> str:
        return self.current_context_dict.get("bootstrap_port", "9092")

    @property
    def bootstrap_domain(self) -> Optional[str]:
        return self.current_context_dict.get("bootstrap_domain", None)

    @property
    def bootstrap_hosts(self) -> List[str]:
        return self.current_context_dict["bootstrap_hosts"].split(",")

    @property
    def schema_registry(self) -> str:
        return self.current_context_dict["schema_registry"]

    @property
    def bootstrap_servers(self):
        return self._generate_urls(self.bootstrap_hosts, self.bootstrap_port, domain=self.bootstrap_domain)

    @property
    def zookeeper_port(self) -> str:
        return self.current_context_dict.get("zookeeper_port", "2181")

    @property
    def zookeeper_hosts(self) -> List[str]:
        return self.current_context_dict.get("zookeeper_hosts").split(",")

    @property
    def zookeeper_domain(self) -> str:
        return self.current_context_dict.get("zookeeper_domain", None)

    @property
    def zookeeper_nodes(self):
        return self._generate_urls(self.zookeeper_hosts, self.zookeeper_port, domain=self.zookeeper_domain)

    @property
    def default_partitions(self) -> int:
        config_dict = self.current_context_dict
        return int(config_dict["default_partitions"])

    @property
    def default_replication_factor(self) -> int:
        config_dict = self.current_context_dict
        return int(config_dict["default_replication_factor"])

    def context_switch(self, context: str):
        click.echo(f"Switched to context: {context}")
        if context not in self.available_contexts:
            raise ContextNotDefinedException(f"{context} not defined in {config_path()}")
        self._update_config("Context", "current", context)

    @staticmethod
    def _generate_urls(hosts: List[str], port: str, *, domain: str = None):
        if domain:
            return [f"{host_name}.{domain}:{port}" for host_name in hosts]
        return [f"{host_name}:{port}" for host_name in hosts]

    def _update_config(self, section: str, key: str, value: str):
        self._cfg.set(section, key, value=value)
        with config_path().open("w") as f:
            self._cfg.write(f)

    def create_pykafka_config(self) -> Dict[str, str]:
        return {"hosts": ",".join(self.bootstrap_servers)}

    def create_confluent_config(
        self, *, debug: bool = False, ssl: bool = False, auth: Optional[Tuple[str, str]] = None
    ) -> Dict[str, str]:

        base_config = {"bootstrap.servers": ",".join(self.bootstrap_servers), "security.protocol": "PLAINTEXT"}
        config = base_config.copy()
        if debug:
            config.update({"debug": "all", "log_level": "2"})

        if ssl:
            config.update({"ssl.ca.location": "/etc/ssl/certs/GlobalSign_Root_CA.pem", "security.protocol": "SSL"})

        if auth:
            user, pw = auth
            config.update({"sasl.mechanisms": "SCRAM-SHA-512", "sasl.username": user, "sasl.password": pw})
            config["security.protocol"] = "SASL_" + config["security.protocol"]
        return config
