import contextlib
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

import click
import yaml
from confluent_kafka.admin import ConfigResource

import esque.validation
from esque.cli import environment
from esque.config.migration import check_config_version
from esque.errors import (
    ConfigException,
    ConfigNotExistsException,
    ContextNotDefinedException,
    MissingSaslParameter,
    UnsupportedSaslMechanism,
)
from esque.helpers import SingletonMeta

PING_TOPIC = "esque-ping"
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

        from esque.cli.output import blue_bold, bold

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

        from esque.cli.output import blue_bold, bold

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

    @contextlib.contextmanager
    def temporary_context(self, context: str):
        old_context = self.current_context
        self.context_switch(context=context)
        yield
        self.context_switch(context=old_context)

    def context_switch(self, context: str):
        if context not in self.available_contexts:
            raise ContextNotDefinedException(f"{context} not defined in {config_path()}")
        self._cfg["current_context"] = context

    def save(self):
        with config_path().open("w") as f:
            yaml.dump(self._cfg, f, default_flow_style=False, sort_keys=False, Dumper=yaml.SafeDumper)

    def create_kafka_python_config(self) -> Dict[str, Any]:
        config = {"bootstrap_servers": self.bootstrap_servers, "security_protocol": self.security_protocol}
        if self.ssl_enabled:
            config.update(self._get_kafka_python_ssl_config())
        if self.sasl_enabled:
            config.update(self._get_kafka_python_sasl_config())
        return config

    def _get_kafka_python_ssl_config(self):
        config = {}
        if self.ssl_params.get("cafile"):
            config["ssl_cafile"] = self.ssl_params["cafile"]
        if self.ssl_params.get("certfile"):
            config["ssl_certfile"] = self.ssl_params["certfile"]
        if self.ssl_params.get("keyfile"):
            config["ssl_keyfile"] = self.ssl_params["keyfile"]
        if self.ssl_params.get("password"):
            config["ssl_password"] = self.ssl_params["password"]
        return config

    def _get_kafka_python_sasl_config(self):
        if self.sasl_mechanism in ("PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"):
            try:
                return {
                    "sasl_mechanism": self.sasl_mechanism,
                    "sasl_plain_username": self.sasl_params["user"],
                    "sasl_plain_password": self.sasl_params["password"],
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
