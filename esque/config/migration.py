import configparser
import io
import logging
import shutil
from pathlib import Path
from typing import Any, Callable, ClassVar, Dict, List, Optional, Tuple, Type, TypeVar, cast

import yaml

from esque.errors import ConfigTooNew, ConfigTooOld

logger = logging.getLogger(__name__)

CURRENT_VERSION = 1
MIGRATORS: Dict[int, Type["BaseMigrator"]] = {}


def check_config_version(config_path: Path) -> None:
    version = get_config_version(config_path)
    if version < CURRENT_VERSION:
        raise ConfigTooOld(version, CURRENT_VERSION)
    elif version > CURRENT_VERSION:
        raise ConfigTooNew(version, CURRENT_VERSION)


def get_config_version(config_path: Path) -> int:
    if config_path.suffix == ".cfg":
        return 0
    with config_path.open("r") as o:
        return yaml.safe_load(o)["version"]


def migrate(config_path: Path) -> Tuple[Path, Optional[Path]]:
    """
    Migrates the config at `config_path` to the new config version.
    :param config_path: The config to migrate.
    :return: (new path, backup)
    """
    old_version = get_config_version(config_path)
    if old_version == CURRENT_VERSION:
        logger.info("Config already at latest version. Nothing to do.")
        return config_path, None
    logger.info(f"Migrating config from version {old_version} to {CURRENT_VERSION}")

    last_path = config_path
    last_data = None
    migrator = None
    backup = None
    for new_version in range(old_version + 1, CURRENT_VERSION + 1):
        migrator = MIGRATORS[new_version](last_path, last_data)
        if backup is None:
            backup = migrator.backup()
        last_path = migrator.new_config_path
        last_data = migrator.new_data
    migrator.save()
    return last_path, backup


class BaseMigrator:
    VERSION: ClassVar[int] = -1

    def __init__(self, old_config_path: Path, old_data: Any = None):
        self._old_config_path = old_config_path
        self._old_data = old_data
        self.new_data = {}
        if not old_data:
            self._load_old_data()
        self._translate_data()

    def _load_old_data(self):
        self.deserialize(self._old_config_path.read_text())

    def _translate_data(self):
        self.new_data["version"] = self.VERSION

    @property
    def new_config_path(self) -> Path:
        return self._old_config_path

    def serialize(self) -> str:
        raise NotImplementedError()

    def deserialize(self, text: str):
        raise NotImplementedError()

    def backup(self) -> Path:
        backup = self._old_config_path.with_name(self._old_config_path.name + ".bak")
        shutil.copy(str(self._old_config_path), str(backup))
        return backup

    def save(self) -> Path:
        self.new_config_path.write_text(self.serialize())
        return self.new_config_path


class V1Migrator(BaseMigrator):
    VERSION = 1

    @property
    def new_config_path(self) -> Path:
        return self._old_config_path.parent / "esque_config.yaml"

    def deserialize(self, text: str):
        self._old_data = configparser.ConfigParser()
        self._old_data.read_string(text)

    def serialize(self) -> str:
        buffer = io.StringIO()
        yaml.dump(self.new_data, buffer, default_flow_style=False, sort_keys=False, Dumper=yaml.SafeDumper)
        return buffer.getvalue()

    def _translate_data(self):
        super()._translate_data()
        self.new_data["current_context"] = self._old_data["Context"]["current"]
        self._translate_contexts()

    def _translate_contexts(self):
        all_contexts = filter(V1Migrator.is_context, self._old_data.values())
        translated_contexts = dict(map(V1Migrator.translate_context, all_contexts))
        self.new_data["contexts"] = translated_contexts

    @staticmethod
    def is_context(section: configparser.SectionProxy) -> bool:
        return section.name.startswith("Context.")

    @staticmethod
    def translate_context(section: configparser.SectionProxy) -> Tuple[str, Dict]:
        data = {"bootstrap_servers": V1Migrator.translate_bootstrap_servers(section)}
        unchanged_fields = ["security_protocol", "schema_registry"]
        for field in unchanged_fields:
            assign_if_present(field, cast(Dict[str, Any], section), data)

        data.update(V1Migrator.get_ssl_settings(section))
        data.update(V1Migrator.get_sasl_settings(section))
        data.update(V1Migrator.translate_defaults(section))

        name = V1Migrator.extract_name_from_section(section)
        return name, data

    @staticmethod
    def translate_defaults(section: configparser.SectionProxy) -> Dict:
        data = {}
        if "default_replication_factor" in section:
            data["replication_factor"] = int(section["default_replication_factor"])
        if "default_partitions" in section:
            data["num_partitions"] = int(section["default_partitions"])

        return {"default_values": data} if data != {} else data

    @staticmethod
    def translate_bootstrap_servers(section: configparser.SectionProxy) -> List[str]:
        server_tpl = "{}"
        if section.get("bootstrap_domain"):
            server_tpl += "." + section["bootstrap_domain"]
        if section.get("bootstrap_port"):
            server_tpl += ":" + section["bootstrap_port"]
        return [server_tpl.format(server.strip()) for server in section["bootstrap_hosts"].split(",")]

    @staticmethod
    def get_ssl_settings(section: configparser.SectionProxy) -> Dict:
        has_ssl_entries = any(entry.startswith("ssl_") for entry in section.keys())
        if not has_ssl_entries:
            return {}

        def translate(ssl_param_name: str) -> str:
            return ssl_param_name[4:]

        ssl_params = ["ssl_cafile", "ssl_certfile", "ssl_keyfile", "ssl_password"]
        data = {}
        for param in ssl_params:
            assign_if_present(param, cast(Dict[str, Any], section), data, translate)
        return {"ssl_params": data}

    @staticmethod
    def get_sasl_settings(section: configparser.SectionProxy) -> Dict:
        has_sasl_entries = any(entry.startswith("sasl_") for entry in section.keys())
        if not has_sasl_entries:
            return {}

        def translate(sasl_param_name: str) -> str:
            return sasl_param_name[5:]

        sasl_params = ["sasl_mechanism", "sasl_user", "sasl_password"]

        data = {translate(param): section[param] for param in sasl_params}
        return {"sasl_params": data}

    @staticmethod
    def extract_name_from_section(section: configparser.SectionProxy) -> str:
        return section.name.split(".", 1)[1]


MIGRATORS[1] = V1Migrator

K = TypeVar("K")


def identity(x: K) -> K:
    return x


def assign_if_present(
    key: K, sourcedict: Dict[K, Any], targetdict: Dict[K, Any], translate: Callable[[K], K] = identity
) -> bool:
    """
    Assigns `sourcedict[key]` to `targetdict[translate(key)]` if and only if sourcedict contains `key`.
    :param key: key to check for
    :param sourcedict: source to check in
    :param targetdict: target to assign to
    :param translate: defaults to identity i.e no translation. Can be used to change key in target dict.
    :return: bool, `key in sourcedict`
    """
    if key in sourcedict:
        targetdict[translate(key)] = sourcedict[key]
        return True
    return False
