import click

from esque import validation
from esque.cli.helpers import edit_yaml
from esque.cli.options import State, default_options
from esque.config import config_path


@click.command("edit", short_help="Edit esque config file.")
@default_options
def config_edit(state: State):
    """Opens the user's esque config file in the default editor."""
    old_yaml = config_path().read_text()
    new_yaml, _ = edit_yaml(old_yaml, validator=validation.validate_esque_config)
    config_path().write_text(new_yaml)
