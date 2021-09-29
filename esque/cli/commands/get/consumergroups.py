import click

from esque.cli.options import State, default_options, output_format_option
from esque.cli.output import format_output
from esque.controller.consumergroup_controller import ConsumerGroupController


@click.command("consumergroups")
@output_format_option
@default_options
@click.option("-p", "--prefix", help="Only list groups starting with this prefix.", default="")
def get_consumergroups(state: State, output_format: str, prefix: str):
    """List all consumer groups."""
    groups = ConsumerGroupController(state.cluster).list_consumer_groups(prefix=prefix)
    click.echo(format_output(groups, output_format))
