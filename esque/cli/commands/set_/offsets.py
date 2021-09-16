import logging

import click

from esque.cli.helpers import ensure_approval, fallback_to_stdin
from esque.cli.options import State, default_options
from esque.cli.output import green_bold, pretty_offset_plan
from esque.controller.consumergroup_controller import ConsumerGroupController


@click.command("offsets")
@click.argument("consumer-id", callback=fallback_to_stdin, type=click.STRING, required=True)
@click.option(
    "-t",
    "--topic-name",
    help="Regular expression describing the topic name (default: all subscribed topics)",
    type=click.STRING,
    required=False,
)
@click.option("--offset-to-value", help="Set offset to the specified value", type=click.INT, required=False)
@click.option("--offset-by-delta", help="Shift offset by specified value", type=click.INT, required=False)
@click.option(
    "--offset-to-timestamp",
    help="Set offset to that of the first message with timestamp on or after the specified timestamp in format "
    "YYYY-MM-DDTHH:mm:ss, i.e. skip all messages before this timestamp.",
    type=click.STRING,
    required=False,
)
@click.option(
    "--offset-from-group", help="Copy all offsets from an existing consumer group.", type=click.STRING, required=False
)
@default_options
def set_offsets(
    state: State,
    consumer_id: str,
    topic_name: str,
    offset_to_value: int,
    offset_by_delta: int,
    offset_to_timestamp: str,
    offset_from_group: str,
):
    """Set consumer group offsets.

    Change or set the offset of a consumer group for a topic, i.e. the message number the consumer group will read next.
    This can be done by specifying an explicit offset (--offset-to-value), a delta to shift the current offset forwards
    or backwards (--offset-by-delta), a timestamp in which the offset of the first message on or after the timestamp is
    taken (--offset-by-timestamp), or a group from which to copy the offsets from. In the case that the consumer group
    reads from more than one topic, a regular expression can be given to specify the offset of which topic to change.
    NOTE: the default is to change the offset for all topics."""
    logger = logging.getLogger(__name__)
    consumergroup_controller = ConsumerGroupController(state.cluster)
    offset_plan = consumergroup_controller.create_consumer_group_offset_change_plan(
        consumer_id=consumer_id,
        topic_name=topic_name if topic_name else ".*",
        offset_to_value=offset_to_value,
        offset_by_delta=offset_by_delta,
        offset_to_timestamp=offset_to_timestamp,
        offset_from_group=offset_from_group,
    )

    if offset_plan and len(offset_plan) > 0:
        click.echo(green_bold("Proposed offset changes: "))
        pretty_offset_plan(offset_plan)
        if ensure_approval("Are you sure?", no_verify=state.no_verify):
            consumergroup_controller.edit_consumer_group_offsets(consumer_id=consumer_id, offset_plan=offset_plan)
    else:
        logger.info("No changes proposed.")
        return
