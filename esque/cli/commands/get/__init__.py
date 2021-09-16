import click

from esque.cli.options import State, default_options

from .brokers import get_brokers
from .consumergroups import get_consumergroups
from .offset import get_offset
from .timestamp import get_timestamp
from .topics import get_topics
from .watermarks import get_watermarks


@click.group(help="Get a quick overview of different resources.", no_args_is_help=True)
@default_options
def get(state: State):
    pass


get.add_command(get_brokers)
get.add_command(get_consumergroups)
get.add_command(get_offset)
get.add_command(get_timestamp)
get.add_command(get_topics)
get.add_command(get_watermarks)
