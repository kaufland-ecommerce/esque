import configparser
import random
import string
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import click

from esque.environment import ESQUE_ENV
from esque.errors import ConfigNotExistsException, ContextNotDefinedException

RANDOM = "".join(random.choices(string.ascii_lowercase, k=8))

PING_TOPIC = f"ping-{RANDOM}"
PING_GROUP_ID = f"ping-{RANDOM}"

SLEEP_INTERVAL = 2


def config_dir() -> Path:
    if ESQUE_ENV == "dev":
        return Path(__file__).parent.parent
    return Path(click.get_app_dir("esque", force_posix=True))


def config_path() -> Path:
    return config_dir() / "esque.cfg"


def sample_config_path() -> Path:
    sample_path = Path(__file__).parent.parent / "config" / "sample_config.cfg"
    return sample_path


class Config:
    def __init__(self):
        self._cfg = configparser.ConfigParser()
        if config_path().exists():
            self._cfg.read(config_path())
        else:
            raise ConfigNotExistsException("No config defined.")

    @property
    def available_contexts(self):
        return sorted(
            [
                key.split(".")[1]
                for key in self._cfg.keys()
                if key.startswith("Context.")
            ]
        )

    @property
    def current_context(self):
        return self._cfg.get("Context", "current")

    @property
    def _current_section(self):
        return f"Context.{self._cfg.get('Context', 'current')}"

    @property
    def current_context_dict(self) -> Dict[str, Any]:
        return {
            option: self._cfg.get(self._current_section, option)
            for option in self._cfg.options(self._current_section)
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
    def bootstrap_servers(self):
        if self.bootstrap_domain:
            return [
                f"{host_name}.{self.bootstrap_domain}:{self.bootstrap_port}"
                for host_name in self.bootstrap_hosts
            ]
        return [
            f"{host_name}:{self.bootstrap_port}" for host_name in self.bootstrap_hosts
        ]

    def context_switch(self, context: str):
        click.echo((f"Switched to context: {context}"))
        if context not in self.available_contexts:
            raise ContextNotDefinedException(
                f"{context} not defined in {config_path()}"
            )
        self._update_config("Context", "current", context)

    def _update_config(self, section: str, key: str, value: str):
        self._cfg.set(section, key, value=value)
        with config_path().open("w") as f:
            self._cfg.write(f)

    def create_pykafka_config(self) -> Dict[str, str]:
        return {"hosts": ",".join(self.bootstrap_servers)}

    def create_confluent_config(
        self,
        *,
        debug: bool = False,
        ssl: bool = False,
        auth: Optional[Tuple[str, str]] = None,
    ) -> Dict[str, str]:

        base_config = {
            "bootstrap.servers": ",".join(self.bootstrap_servers),
            "security.protocol": "PLAINTEXT",
        }
        config = base_config.copy()
        if debug:
            config.update({"debug": "all", "log_level": "2"})

        if ssl:
            config.update(
                {
                    "ssl.ca.location": "/etc/ssl/certs/GlobalSign_Root_CA.pem",
                    "security.protocol": "SSL",
                }
            )

        if auth:
            user, pw = auth
            config.update(
                {
                    "sasl.mechanisms": "SCRAM-SHA-512",
                    "sasl.username": user,
                    "sasl.password": pw,
                }
            )
            config["security.protocol"] = "SASL_" + config["security.protocol"]
        return config
