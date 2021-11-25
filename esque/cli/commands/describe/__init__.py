import click

from esque.cli.options import State, default_options

from .broker import describe_broker
from .consumergroup import describe_consumergroup
from .topic import describe_topic


@click.group(help="Get detailed information about a resource.", no_args_is_help=True)
@default_options
def describe(state: State):
    pass


describe.add_command(describe_broker)
describe.add_command(describe_consumergroup)
describe.add_command(describe_topic)
