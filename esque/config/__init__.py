import logging
import random
import string
from pathlib import Path
from typing import Any, Dict, List, Optional

import click
import yaml

from esque.cli import environment
from esque.config.migration import check_config_version
from esque.errors import (
    ConfigException,
    ConfigNotExistsException,
    ContextNotDefinedException,
    MissingSaslParameter,
    UnsupportedSaslMechanism,
)
from esque.validation import validate_esque_config
from pykafka import SslConfig
from pykafka.sasl_authenticators import BaseAuthenticator, PlainAuthenticator, ScramAuthenticator

RANDOM = "".join(random.choices(string.ascii_lowercase, k=8))
PING_TOPIC = f"ping-{RANDOM}"
PING_GROUP_ID = f"ping-{RANDOM}"
SLEEP_INTERVAL = 2
SUPPORTED_SASL_MECHANISMS = ("PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512")
log = logging.getLogger(__name__)


def config_dir() -> Path:
    return _config_dir()


def config_path() -> Path:
    return _config_path()


# create functions we can mock during tests
def _config_path() -> Path:
    if environment.ESQUE_CONF_PATH:
        return Path(environment.ESQUE_CONF_PATH)
    return config_dir() / "esque.cfg"


def _config_dir() -> Path:
    return Path(click.get_app_dir("esque", force_posix=True))


def sample_config_path() -> Path:
    return Path(__file__).parent / "sample_config.yaml"


class Config:
    def __init__(self):
        if not config_path().exists():
            raise ConfigNotExistsException()
        check_config_version(config_path())
        self._cfg = yaml.safe_load(config_path().read_text())
        validate_esque_config(self._cfg)
        self._current_dict: Optional[Dict[str, str]] = None

    @property
    def available_contexts(self) -> List[str]:
        return sorted(self._cfg["contexts"])

    @property
    def default_values(self) -> Dict[str, Any]:
        return self.current_context_dict.get("default_values", {})

    @property
    def current_context(self) -> str:
        return self._cfg["current_context"]

    @property
    def current_context_dict(self) -> Dict[str, Any]:
        return self._cfg["contexts"][self.current_context]

    @property
    def schema_registry(self) -> str:
        config_dict = self.current_context_dict
        return config_dict["schema_registry"]

    @property
    def bootstrap_servers(self) -> List[str]:
        return self.current_context_dict["bootstrap_servers"]

    @property
    def default_num_partitions(self) -> int:
        return self.default_values.get("num_partitions", 1)

    @property
    def default_replication_factor(self) -> int:
        return self.default_values.get("replication_factor", min(len(self.bootstrap_servers), 3))

    @property
    def sasl_mechanism(self) -> str:
        if "sasl_mechanism" not in self.current_context_dict:
            raise MissingSaslParameter(f"No sasl mechanism configured, valid values are {SUPPORTED_SASL_MECHANISMS}")
        return self.current_context_dict["sasl_mechanism"]

    @property
    def sasl_enabled(self) -> bool:
        return "SASL" in self.security_protocol

    @property
    def sasl_params(self) -> Dict[str, str]:
        return self.current_context_dict.get("sasl_params", {})

    @property
    def ssl_params(self) -> Dict[str, str]:
        return self.current_context_dict.get("ssl_params", {})

    @property
    def ssl_enabled(self) -> bool:
        return "SSL" in self.security_protocol

    @property
    def security_protocol(self) -> str:
        protocol = self.current_context_dict.get("security_protocol", "PLAINTEXT")
        if "SASL" in protocol and "sasl_params" not in self.current_context_dict:
            msg = (
                f"Security protocol {protocol} contains 'SASL' indicating that you want to connect to "
                "a SASL enabled endpoint but you didn't specify sasl parameters.\n"
                f"Run `esque config edit` and add `sasl_params` to section 'contexts.{self.current_context}'.\n"
                f"Valid mechanisms are {SUPPORTED_SASL_MECHANISMS}."
            )
            raise ConfigException(msg)
        return protocol

    def context_switch(self, context: str):
        if context not in self.available_contexts:
            raise ContextNotDefinedException(f"{context} not defined in {config_path()}")
        self._cfg["current_context"] = context
        self._dump_config()

    def _dump_config(self):
        with config_path().open("w") as f:
            yaml.dump(self._cfg, f, default_flow_style=False, sort_keys=False, Dumper=yaml.SafeDumper)

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
                    "sasl.username": self.sasl_params["user"],
                    "sasl.password": self.sasl_params["password"],
                }
            except KeyError as e:
                msg = f"SASL mechanism {self.sasl_mechanism} requires parameter {e.args[0]}.\n"
                msg += f"Please run `esque config edit` and add it to section [{self.current_context}]."
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
            rdk_conf["ssl.ca.location"] = self.ssl_params["cafile"]
        if self.current_context_dict.get("ssl_certfile"):
            rdk_conf["ssl.certificate.location"] = self.ssl_params["certfile"]
        if self.current_context_dict.get("ssl_keyfile"):
            rdk_conf["ssl.key.location"] = self.ssl_params["keyfile"]
        if self.current_context_dict.get("ssl_password"):
            rdk_conf["ssl.key.password"] = self.ssl_params["password"]
        return rdk_conf

    def _get_pykafka_ssl_conf(self) -> Optional[SslConfig]:
        if not self.ssl_enabled:
            return None
        ssl_params = {"cafile": self.current_context_dict.get("ssl_cafile", None)}
        if self.current_context_dict.get("ssl_certfile"):
            ssl_params["certfile"] = self.ssl_params["certfile"]
        if self.current_context_dict.get("ssl_keyfile"):
            ssl_params["keyfile"] = self.ssl_params["keyfile"]
        if self.current_context_dict.get("ssl_password"):
            ssl_params["password"] = self.ssl_params["password"]
        return SslConfig(**ssl_params)

    def _get_pykafka_authenticator(self) -> BaseAuthenticator:
        try:
            if self.sasl_mechanism == "PLAIN":
                return PlainAuthenticator(
                    user=self.sasl_params["user"],
                    password=self.sasl_params["password"],
                    security_protocol=self.security_protocol,
                )
            elif self.sasl_mechanism in ("SCRAM-SHA-256", "SCRAM-SHA-512"):
                return ScramAuthenticator(
                    self.sasl_mechanism,
                    user=self.sasl_params["user"],
                    password=self.sasl_params["password"],
                    security_protocol=self.security_protocol,
                )
            else:
                raise UnsupportedSaslMechanism(
                    f"SASL mechanism {self.sasl_mechanism} is currently not supported by esque. "
                    f"Supported meachnisms are {SUPPORTED_SASL_MECHANISMS}."
                )
        except KeyError as e:
            msg = f"SASL mechanism {self.sasl_mechanism} requires parameter {e.args[0]}.\n"
            msg += f"Please run `esque config edit` and add it to section [{self.current_context}]."
            raise MissingSaslParameter(msg)
