import configparser
import logging
import random
import string
from pathlib import Path
from typing import Any, Dict, List, Optional

import click

from esque.cli import environment
from esque.errors import (
    ConfigException,
    ConfigNotExistsException,
    ContextNotDefinedException,
    MissingSaslParameter,
    UnsupportedSaslMechanism,
)
from pykafka import SslConfig
from pykafka.sasl_authenticators import BaseAuthenticator, PlainAuthenticator, ScramAuthenticator

RANDOM = "".join(random.choices(string.ascii_lowercase, k=8))
PING_TOPIC = f"ping-{RANDOM}"
PING_GROUP_ID = f"ping-{RANDOM}"
SLEEP_INTERVAL = 2
SUPPORTED_SASL_MECHANISMS = ("PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512")
log = logging.getLogger(__name__)


def config_dir() -> Path:
    return Path(click.get_app_dir("esque", force_posix=True))


def config_path() -> Path:
    if environment.ESQUE_CONF_PATH:
        return Path(environment.ESQUE_CONF_PATH)
    return config_dir() / "esque.cfg"


def sample_config_path() -> Path:
    return Path(__file__).parent / "sample_config.cfg"


class Config:
    def __init__(self):
        self._cfg = configparser.ConfigParser()
        self._current_dict: Optional[Dict[str, str]] = None
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
        if self._current_dict is None:
            self._current_dict = self._get_section_dict(self._current_section)
        return self._current_dict

    def _get_section_dict(self, section) -> Dict[str, str]:
        if not self._cfg.has_section(section):
            log.debug(f"Section {section} not found in config.")
            return {}

        return {option: self._cfg.get(section, option) for option in self._cfg.options(section)}

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
    def sasl_mechanism(self) -> str:
        if "sasl_mechanism" not in self.current_context_dict:
            raise MissingSaslParameter(f"No sasl mechanism configured, valid values are {SUPPORTED_SASL_MECHANISMS}")
        return self.current_context_dict["sasl_mechanism"]

    @property
    def sasl_enabled(self) -> bool:
        return "SASL" in self.security_protocol

    @property
    def ssl_enabled(self) -> bool:
        return "SSL" in self.security_protocol

    @property
    def security_protocol(self) -> str:
        protocol = self.current_context_dict.get("security_protocol", "PLAINTEXT")
        if "SASL" in protocol and "sasl_mechanism" not in self.current_context_dict:
            msg = (
                f"Security protocol {protocol} contains 'SASL' indicating that you want to connect to "
                "a SASL enabled endpoint but you didn't specify a sasl mechanism.\n"
                f"Run `esque edit config` and add `sasl_mechanism` to section '[{self._current_section}]'.\n"
                f"Valid mechanisms are {SUPPORTED_SASL_MECHANISMS}."
            )
            raise ConfigException(msg)
        return protocol

    def context_switch(self, context: str):
        if context not in self.available_contexts:
            raise ContextNotDefinedException(f"{context} not defined in {config_path()}")
        self._update_config("Context", "current", context)
        self._current_dict = None

    def _update_config(self, section: str, key: str, value: str):
        self._cfg.set(section, key, value=value)
        with config_path().open("w") as f:
            self._cfg.write(f)

    def create_pykafka_config(self) -> Dict[str, Any]:
        config = {"hosts": ",".join(self.bootstrap_servers)}
        if self.sasl_enabled:
            config["sasl_authenticator"] = self._get_pykafka_authenticator()
        if self.ssl_enabled:
            config["ssl_config"] = self._get_pykafka_ssl_conf()
        log.debug(f"Created pykafka config: {config}")
        return config

    def create_confluent_config(self, *, debug: bool = False) -> Dict[str, str]:
        base_config = {
            "bootstrap.servers": ",".join(self.bootstrap_servers),
            "security.protocol": self.security_protocol,
        }
        config = base_config.copy()
        if debug:
            config.update({"debug": "all", "log_level": "2"})
        if self.sasl_enabled:
            config.update(self._get_confluent_sasl_config())
        if self.ssl_enabled:
            config.update(self._get_confluent_ssl_config())
        log.debug(f"Created confluent config: {config}")
        return config

    def _get_confluent_sasl_config(self) -> Dict[str, str]:
        if self.sasl_mechanism in ("PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"):
            try:
                return {
                    "sasl.mechanisms": self.sasl_mechanism,
                    "sasl.username": self.current_context_dict["sasl_user"],
                    "sasl.password": self.current_context_dict["sasl_password"],
                }
            except KeyError as e:
                msg = f"SASL mechanism {self.sasl_mechanism} requires parameter {e.args[0]}.\n"
                msg += f"Please run `esque edit config` and add it to section [{self.current_context}]."
                raise MissingSaslParameter(msg)
        else:
            raise UnsupportedSaslMechanism(
                f"SASL mechanism {self.sasl_mechanism} is currently not supported by esque. "
                f"Supported meachnisms are {SUPPORTED_SASL_MECHANISMS}."
            )

    def _get_confluent_ssl_config(self) -> Dict[str, str]:
        if not self.ssl_enabled:
            return {}
        rdk_conf = {}
        if self.current_context_dict.get("ssl_cafile"):
            rdk_conf["ssl.ca.location"] = self.current_context_dict["ssl_cafile"]
        if self.current_context_dict.get("ssl_certfile"):
            rdk_conf["ssl.certificate.location"] = self.current_context_dict["ssl_certfile"]
        if self.current_context_dict.get("ssl_keyfile"):
            rdk_conf["ssl.key.location"] = self.current_context_dict["ssl_keyfile"]
        if self.current_context_dict.get("ssl_password"):
            rdk_conf["ssl.key.password"] = self.current_context_dict["ssl_password"]
        return rdk_conf

    def _get_pykafka_ssl_conf(self) -> Optional[SslConfig]:
        if not self.ssl_enabled:
            return None
        ssl_params = {"cafile": self.current_context_dict.get("ssl_cafile", None)}
        if self.current_context_dict.get("ssl_certfile"):
            ssl_params["certfile"] = self.current_context_dict["ssl_certfile"]
        if self.current_context_dict.get("ssl_keyfile"):
            ssl_params["keyfile"] = self.current_context_dict["ssl_keyfile"]
        if self.current_context_dict.get("ssl_password"):
            ssl_params["password"] = self.current_context_dict["ssl_password"]
        return SslConfig(**ssl_params)

    def _get_pykafka_authenticator(self) -> BaseAuthenticator:
        try:
            if self.sasl_mechanism == "PLAIN":
                return PlainAuthenticator(
                    user=self.current_context_dict["sasl_user"],
                    password=self.current_context_dict["sasl_password"],
                    security_protocol=self.security_protocol,
                )
            elif self.sasl_mechanism in ("SCRAM-SHA-256", "SCRAM-SHA-512"):
                return ScramAuthenticator(
                    self.sasl_mechanism,
                    user=self.current_context_dict["sasl_user"],
                    password=self.current_context_dict["sasl_password"],
                    security_protocol=self.security_protocol,
                )
            else:
                raise UnsupportedSaslMechanism(
                    f"SASL mechanism {self.sasl_mechanism} is currently not supported by esque. "
                    f"Supported meachnisms are {SUPPORTED_SASL_MECHANISMS}."
                )
        except KeyError as e:
            msg = f"SASL mechanism {self.sasl_mechanism} requires parameter {e.args[0]}.\n"
            msg += f"Please run `esque edit config` and add it to section [{self.current_context}]."
            raise MissingSaslParameter(msg)
