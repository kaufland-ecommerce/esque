import click

from esque.cli.options import State, default_options

from .offsets import edit_offsets
from .topic import edit_topic


@click.group(help="Edit a resource.", no_args_is_help=True)
@default_options
def edit(state: State):
    pass


edit.add_command(edit_offsets)
edit.add_command(edit_topic)
