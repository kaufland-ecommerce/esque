import logging
import random
import string
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional

import click
import yaml
from confluent_kafka.admin import ConfigResource
from pykafka import SslConfig

import esque.validation
from esque.cli import environment
from esque.config.migration import check_config_version
from esque.errors import (
    ConfigException,
    ConfigNotExistsException,
    ContextNotDefinedException,
    ExceptionWithMessage,
    MissingSaslParameter,
    UnsupportedSaslMechanism,
)
from esque.helpers import SingletonMeta

if TYPE_CHECKING:
    try:
        from pykafka.sasl_authenticators import BaseAuthenticator
    except ImportError:
        raise ImportError(
            "Please install our pykafka fork:\n"
            "pip install -U git+https://github.com/real-digital/pykafka.git@feature/sasl-scram-support"
        )

RANDOM = "".join(random.choices(string.ascii_lowercase, k=8))
PING_TOPIC = f"ping-{RANDOM}"
ESQUE_GROUP_ID = "esque-client"
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
    legacy_path = config_dir() / "esque.cfg"
    current_path = config_dir() / "esque_config.yaml"
    if legacy_path.exists() and not current_path.exists():
        return legacy_path
    return current_path


def _config_dir() -> Path:
    return Path(click.get_app_dir("esque", force_posix=True))


def sample_config_path() -> Path:
    return Path(__file__).parent / "sample_config.yaml"


class Config(metaclass=SingletonMeta):
    def __init__(self, *, disable_validation=False):
        if not config_path().exists():
            raise ConfigNotExistsException()
        check_config_version(config_path())
        self._cfg = yaml.safe_load(config_path().read_text())
        if not disable_validation:
            esque.validation.validate_esque_config(self._cfg)
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
        if "num_partitions" in self.default_values:
            return self.default_values["num_partitions"]

        from esque.cli.output import bold, blue_bold

        try:
            log.warning("Fetching default number of partitions from broker.")
            log.warning(f"Run `esque config edit` and add `{bold('num_partitions')}` to your defaults to avoid this.")
            default = int(self._get_broker_setting("num.partitions"))
            log.warning(f"Cluster default is {blue_bold(str(default))}.")
        except Exception as e:
            default = 1
            log.warning(f"Fetching default from broker failed. Falling back to {default}")
            log.info(type(e).__name__, exc_info=True)
        return default

    @staticmethod
    def _get_broker_setting(self, setting: str) -> str:
        from esque.cluster import Cluster

        cluster = Cluster()
        brokers = cluster.brokers
        config = cluster.retrieve_config(ConfigResource.Type.BROKER, brokers[0]["id"])
        return config[setting]

    @property
    def default_replication_factor(self) -> int:
        if "replication_factor" in self.default_values:
            return self.default_values["replication_factor"]

        from esque.cli.output import bold, blue_bold

        try:
            log.warning("Fetching default replication factor from broker.")
            log.warning(
                f"Run `esque config edit` and add `{bold('replication_factor')}` to your defaults to avoid this."
            )
            default = int(self._get_broker_setting("default.replication.factor"))
            log.warning(f"Cluster default is {blue_bold(str(default))}.")
        except Exception as e:
            default = min(len(self.bootstrap_servers), 3)
            log.warning(f"Fetching default from broker failed. Falling back to {default}")
            log.info(type(e).__name__, exc_info=True)
        return default

    @property
    def sasl_mechanism(self) -> str:
        if "mechanism" not in self.sasl_params:
            raise MissingSaslParameter(f"No sasl mechanism configured, valid values are {SUPPORTED_SASL_MECHANISMS}")
        return self.sasl_params["mechanism"].upper()

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
        protocol = self.current_context_dict.get("security_protocol", "PLAINTEXT").upper()
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

    def save(self):
        with config_path().open("w") as f:
            yaml.dump(self._cfg, f, default_flow_style=False, sort_keys=False, Dumper=yaml.SafeDumper)

    def create_pykafka_config(self) -> Dict[str, Any]:
        config = {"hosts": ",".join(self.bootstrap_servers), "exclude_internal_topics": False}
        if self.sasl_enabled:
            config["sasl_authenticator"] = self._get_pykafka_authenticator()
        if self.ssl_enabled:
            config["ssl_config"] = self._get_pykafka_ssl_conf()
        log.debug(f"Created pykafka config: {config}")
        return config

    def create_confluent_config(self, *, debug: bool = False, include_schema_registry: bool = False) -> Dict[str, str]:
        config = {"bootstrap.servers": ",".join(self.bootstrap_servers), "security.protocol": self.security_protocol}
        if debug:
            config.update({"debug": "all", "log_level": "2"})

        if include_schema_registry:
            config["schema.registry.url"] = self.schema_registry

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
        if self.ssl_params.get("cafile"):
            rdk_conf["ssl.ca.location"] = self.ssl_params["cafile"]
        if self.ssl_params.get("certfile"):
            rdk_conf["ssl.certificate.location"] = self.ssl_params["certfile"]
        if self.ssl_params.get("keyfile"):
            rdk_conf["ssl.key.location"] = self.ssl_params["keyfile"]
        if self.ssl_params.get("password"):
            rdk_conf["ssl.key.password"] = self.ssl_params["password"]
        return rdk_conf

    def _get_pykafka_ssl_conf(self) -> Optional[SslConfig]:
        if not self.ssl_enabled:
            return None
        ssl_params = {"cafile": self.ssl_params.get("cafile", None)}
        if self.ssl_params.get("certfile"):
            ssl_params["certfile"] = self.ssl_params["certfile"]
        if self.ssl_params.get("keyfile"):
            ssl_params["keyfile"] = self.ssl_params["keyfile"]
        if self.ssl_params.get("password"):
            ssl_params["password"] = self.ssl_params["password"]
        return SslConfig(**ssl_params)

    def _get_pykafka_authenticator(self) -> "BaseAuthenticator":
        try:
            from pykafka.sasl_authenticators import PlainAuthenticator, ScramAuthenticator
        except ImportError:
            raise ExceptionWithMessage(
                "In order to support SASL you'll need to install our fork of pykafka.\n"
                "Please run:\n"
                "    pip install -U git+https://github.com/real-digital/pykafka.git@feature/sasl-scram-support"
            )

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
