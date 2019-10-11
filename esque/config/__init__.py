import configparser
import random
import string
from pathlib import Path
from typing import Any, Dict, List, Optional

import click
from pykafka.sasl_authenticators import BaseAuthenticator, ScramAuthenticator, PlainAuthenticator

from esque.cli.environment import ESQUE_CONF_PATH
from esque.errors import ConfigNotExistsException, ContextNotDefinedException, MissingSaslParameter, UnsupportedSaslMechanism

RANDOM = "".join(random.choices(string.ascii_lowercase, k=8))
PING_TOPIC = f"ping-{RANDOM}"
PING_GROUP_ID = f"ping-{RANDOM}"
SLEEP_INTERVAL = 2
SUPPORTED_SASL_MECHANISMS = ("PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512")


def config_dir() -> Path:
    return Path(click.get_app_dir("esque", force_posix=True))


def config_path() -> Path:
    if ESQUE_CONF_PATH:
        return Path(ESQUE_CONF_PATH)
    return config_dir() / "esque.cfg"


def sample_config_path() -> Path:
    return Path(__file__).parent / "sample_config.cfg"


class Config:
    def __init__(self):
        self._cfg = configparser.ConfigParser()
        if config_path().exists():
            self._cfg.read(config_path())
        else:
            raise ConfigNotExistsException()

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
        if "bootstrap_port" in self.current_context_dict.keys():
            return self.current_context_dict["bootstrap_port"]
        return "9092"

    @property
    def bootstrap_domain(self) -> Optional[str]:
        config_dict = self.current_context_dict
        if "bootstrap_domain" in config_dict:
            return config_dict["bootstrap_domain"]
        return None

    @property
    def bootstrap_hosts(self) -> List[str]:
        config_dict = self.current_context_dict
        return config_dict["bootstrap_hosts"].split(",")

    @property
    def schema_registry(self) -> str:
        config_dict = self.current_context_dict
        return config_dict["schema_registry"]

    @property
    def bootstrap_servers(self):
        if self.bootstrap_domain:
            return [f"{host_name}.{self.bootstrap_domain}:{self.bootstrap_port}" for host_name in self.bootstrap_hosts]
        return [f"{host_name}:{self.bootstrap_port}" for host_name in self.bootstrap_hosts]

    @property
    def default_partitions(self) -> int:
        config_dict = self.current_context_dict
        return int(config_dict["default_partitions"])

    @property
    def default_replication_factor(self) -> int:
        config_dict = self.current_context_dict
        return int(config_dict["default_replication_factor"])

    @property
    def sasl_mechanism(self) -> Optional[str]:
        if self.sasl_params is None:
            return None
        if "mechanism" not in self.sasl_params:
            raise MissingSaslParameter(f"No sasl mechanism configured, valid values are {SUPPORTED_SASL_MECHANISMS}")
        return self.sasl_params["mechanism"] if self.sasl_params else None

    @property
    def sasl_params(self) -> Optional[Dict[str, str]]:
        return self.current_context_dict.get("sasl", None)

    @property
    def security_protocol(self) -> str:
        return self.current_context_dict["security_protocol"]

    def context_switch(self, context: str):
        click.echo(f"Switched to context: {context}")
        if context not in self.available_contexts:
            raise ContextNotDefinedException(f"{context} not defined in {config_path()}")
        self._update_config("Context", "current", context)

    def _update_config(self, section: str, key: str, value: str):
        self._cfg.set(section, key, value=value)
        with config_path().open("w") as f:
            self._cfg.write(f)

    def create_pykafka_config(self) -> Dict[str, str]:
        return {"hosts": ",".join(self.bootstrap_servers), "sasl_authenticator": self.get_pykafka_authenticator()}

    def create_confluent_config(self, *, debug: bool = False) -> Dict[str, str]:

        base_config = {"bootstrap.servers": ",".join(self.bootstrap_servers), "security.protocol": "PLAINTEXT"}
        config = base_config.copy()
        if debug:
            config.update({"debug": "all", "log_level": "2"})

        config.update(self._get_confluent_sasl_config())
        return config

    def _get_confluent_sasl_config(self) -> Dict[str, str]:
        if self.sasl_mechanism is None:
            return {}
        if self.sasl_mechanism in ("PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"):
            try:
                return {
                    "sasl.mechanisms": self.sasl_mechanism,
                    "sasl.username": self.sasl_params["user"],
                    "sasl.password": self.sasl_params["password"],
                    "security.protocol": self.security_protocol,
                }
            except KeyError as e:
                raise MissingSaslParameter(f"SASL mechanism {self.sasl_mechanism} requires parameter {e.args[0]}")
        else:
            raise UnsupportedSaslMechanism(
                f"SASL mechanism {self.sasl_mechanism} is currently not supported by esque. "
                f"Supported meachnisms are {SUPPORTED_SASL_MECHANISMS}."
            )

    def get_pykafka_authenticator(self) -> BaseAuthenticator:
        try:
            if self.sasl_mechanism == "PLAIN":
                return PlainAuthenticator(user=self.sasl_params["user"], password=self.sasl_params["password"])
            if self.sasl_mechanism in ("SCRAM-SHA-256", "SCRAM-SHA-512"):
                return ScramAuthenticator(
                    self.sasl_mechanism, user=self.sasl_params["user"], password=self.sasl_params["password"]
                )
            else:
                raise UnsupportedSaslMechanism(
                    f"SASL mechanism {self.sasl_mechanism} is currently not supported by esque. "
                    f"Supported meachnisms are {SUPPORTED_SASL_MECHANISMS}."
                )
        except KeyError as e:
            raise MissingSaslParameter(f"SASL mechanism {self.sasl_mechanism} requires parameter {e.args[0]}")
