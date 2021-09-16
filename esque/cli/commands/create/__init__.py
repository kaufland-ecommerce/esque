import click

from esque.cli.options import State, default_options

from .consumergroup import create_consumergroup
from .topic import create_topic


@click.group(help="Create a new instance of a resource.", no_args_is_help=True)
@default_options
def create(state: State):
    pass


create.add_command(create_consumergroup)
create.add_command(create_topic)
