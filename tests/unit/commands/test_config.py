from pathlib import Path
from unittest import mock

import click
import yaml
from click.testing import CliRunner

from esque.cli.commands.config.edit import config_edit
from esque.cli.commands.config.fix import config_fix
from esque.cli.commands.config.migrate import config_migrate
from esque.config import Config, migration
from esque.config.migration import CURRENT_VERSION, get_config_version, migrate
from tests.conftest import LOAD_BROKEN_CONFIG, config_loader


def test_migrate_config(mocker: mock, interactive_cli_runner: CliRunner, load_config: config_loader):
    conf_path, old_conf_text = load_config(0)
    assert get_config_version(conf_path) == CURRENT_VERSION - 1
    mocker.patch("esque.config._config_dir", return_value=conf_path.parent)

    new_conf_path = conf_path
    backup = None

    def migration_wrapper(config_path: Path):
        nonlocal new_conf_path, backup
        new_conf_path, backup = migrate(config_path)
        return new_conf_path, backup

    mocker.patch.object(migration, "migrate", wraps=migration_wrapper)

    result = interactive_cli_runner.invoke(config_migrate, catch_exceptions=False)

    assert result.exit_code == 0
    assert get_config_version(new_conf_path) == CURRENT_VERSION
    assert backup.read_text() == old_conf_text


def test_edit_config(mocker: mock, interactive_cli_runner: CliRunner, load_config: config_loader):
    conf_path, old_conf_text = load_config()
    data = yaml.safe_load(old_conf_text)
    data["contexts"]["dupe"] = data["contexts"]["context_1"]
    mocker.patch.object(click, "edit", return_value=yaml.dump(data))

    result = interactive_cli_runner.invoke(config_edit, catch_exceptions=False)
    assert result.exit_code == 0
    config = Config()
    assert "dupe" in config.available_contexts


def test_fix_missing_context_config(interactive_cli_runner: CliRunner, load_config: config_loader):
    load_config(LOAD_BROKEN_CONFIG)

    _cfg = Config(disable_validation=True)
    assert _cfg.current_context not in _cfg.available_contexts

    interactive_cli_runner.invoke(config_fix, catch_exceptions=False)

    _cfg = Config.get_instance()

    assert _cfg.current_context in _cfg.available_contexts
