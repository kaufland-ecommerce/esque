import click

from esque.cli.autocomplete import list_topics
from esque.cli.helpers import fallback_to_stdin
from esque.cli.options import State, default_options, output_format_option
from esque.cli.output import output_offset_data
from esque.errors import ValidationException


@click.command("timestamp")
@click.argument(
    "topic-name", metavar="TOPIC_NAME", callback=fallback_to_stdin, type=click.STRING, shell_complete=list_topics
)
@click.argument("offset", metavar="OFFSET")
@output_format_option
@default_options
def get_timestamp(state: State, topic_name: str, offset: str, output_format: str):
    """Get Timestamps for given offset.

    Gets the timestamp for the message(s) at or right after OFFSET in topic TOPIC_NAME.
    If the topic has multiple partitions, the OFFSET wil be used for every partition.
    If there is no message at OFFSET, the next available offset will be used.
    If there is no message at all after OFFSET, offset will be `-1` and timestamp will be `None` in the output for the
    corresponding partition.
    In the Kafka world, -1 corresponds to the position after the last known message i.e. the end of the topic partition
    _not including_ the last message in the partition.

    OFFSET must be a positive integer or `first`/`last` in order to read the first or last message from the partition(s).
    """
    if isinstance(offset, str) and (offset.lower() in ["first", "last"]):
        offset = offset.lower()
    elif offset.isdigit():
        offset = int(offset)
    else:
        raise ValidationException('Offset must be a positive integer, "first" or "last"')

    offsets = state.cluster.topic_controller.get_timestamp_of_closest_offset(topic_name=topic_name, offset=offset)

    output_offset_data(offsets, output_format)
