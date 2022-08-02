import click
import pendulum

from esque.cli.autocomplete import list_topics
from esque.cli.options import State, default_options, output_format_option
from esque.cli.output import output_offset_data


@click.command("offset")
@click.argument("topic-name", metavar="TOPIC_NAME", shell_complete=list_topics)
@click.argument("timestamp-ms", type=int, metavar="TIMESTAMP_MS")
@output_format_option
@default_options
def get_offset(state: State, output_format: str, topic_name: str, timestamp_ms: int):
    """Find offset for given timestamp.

    \b
    Gets the offsets of the message(s) in TOPIC_NAME whose timestamps are at, or right after, the given TIMESTAMP.
    If the topic has multiple partitions, the TIMESTAMP_MS wil be used for every partition.
    If there is no message at TIMESTAMP_MS, the next available one will be used.
    If there is no message at all after TIMESTAMP_MS, offset will be `-1` and timestamp will be `None` in the output for
    the corresponding partition.
    In the Kafka world, -1 corresponds to the position after the last known message i.e. the end of the topic partition
    _not including_ the last message in the partition.

    TIMESTAMP_MS must be in milliseconds since epoch (UTC).

    \b
    EXAMPLES:
    # Check which offset(s) of topic "mytopic" correspond to yesterday noon. Hint "date" supports a wide range
    # of strings for defining dates. Also things like "2 hours ago" or real timestamps like "2021-01-01T00:00:00+00:00".
    esque get offset mytopic $(date -d "yesterday 12pm" +%s000)
    """

    offsets = state.cluster.topic_controller.get_offsets_closest_to_timestamp(
        topic_name=topic_name, timestamp=pendulum.from_timestamp(timestamp_ms / 1000)
    )
    output_offset_data(offsets, output_format)
