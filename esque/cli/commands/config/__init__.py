import click

from esque.cli.options import State, default_options

from .autocomplete import config_autocomplete
from .edit import config_edit
from .fix import config_fix
from .migrate import config_migrate
from .recreate import config_recreate


@click.group(help="Configuration-related options.", no_args_is_help=True)
@default_options
def config(state: State):
    pass


config.add_command(config_autocomplete)
config.add_command(config_edit)
config.add_command(config_fix)
config.add_command(config_migrate)
config.add_command(config_recreate)
