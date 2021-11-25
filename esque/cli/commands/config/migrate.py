import click

from esque.cli.options import State, default_options
from esque.config import config_path, migration


@click.command("migrate")
@default_options
def config_migrate(state: State):
    """Migrate esque config to current version.

    If the user's esque config file is from a previous version, migrate it to the current version.
    A backup of the original config file is created with the same name and the extension .bak"""
    new_path, backup = migration.migrate(config_path())
    click.echo(f"Your config has been migrated and is now at {new_path}. A backup has been created at {backup}.")
