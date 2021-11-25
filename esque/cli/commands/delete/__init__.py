import click

from esque.cli.options import State, default_options

from .consumergroup import delete_consumergroup
from .topic import delete_topic
from .topics import delete_topics


@click.group(help="Delete a resource.", no_args_is_help=True)
@default_options
def delete(state: State):
    pass


delete.add_command(delete_consumergroup)
delete.add_command(delete_topic)
delete.add_command(delete_topics)
