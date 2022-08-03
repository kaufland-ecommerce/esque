import click

from esque.cli.autocomplete import list_consumergroups
from esque.cli.helpers import fallback_to_stdin
from esque.cli.options import State, default_options, output_format_option
from esque.cli.output import format_output
from esque.controller.consumergroup_controller import ConsumerGroupController


@click.command("consumergroup", short_help="Describe a consumer group.")
@click.argument("consumergroup-id", callback=fallback_to_stdin, shell_complete=list_consumergroups, required=True)
@click.option(
    "--all-partitions",
    help="List status for all topic partitions instead of just summarizing each topic.",
    default=False,
    is_flag=True,
)
@click.option("--timestamps", help="Include timestamps for all topic partitions.", default=False, is_flag=True)
@output_format_option
@default_options
def describe_consumergroup(
    state: State, consumergroup_id: str, all_partitions: bool, timestamps: bool, output_format: str
):
    """Return information on group coordinator, offsets, watermarks, lag, and various metadata
    for consumer group CONSUMER_GROUP."""
    consumer_group = ConsumerGroupController(state.cluster).get_consumer_group(consumergroup_id)
    consumer_group_desc = consumer_group.describe(partitions=all_partitions, timestamps=timestamps)

    click.echo(format_output(consumer_group_desc, output_format))
