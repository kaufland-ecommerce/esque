import sys
import time
from pathlib import Path

import click

from esque.cli.autocomplete import list_consumergroups, list_contexts, list_topics
from esque.cli.helpers import attrgetter
from esque.cli.options import State, default_options
from esque.cli.output import blue_bold, bold
from esque.clients.consumer import consume_to_file_ordered, consume_to_files
from esque.config import ESQUE_GROUP_ID
from esque.errors import TopicDoesNotExistException


@click.command("consume")
@click.argument("topic", autocompletion=list_topics)
@click.option(
    "-d", "--directory", metavar="<directory>", help="Sets the directory to write the messages to.", type=click.STRING
)
@click.option(
    "-f",
    "--from",
    "from_context",
    metavar="<source_ctx>",
    help="Source context. If not provided, the current context will be used.",
    autocompletion=list_contexts,
    type=click.STRING,
    required=False,
)
@click.option(
    "-n", "--number", metavar="<n>", help="Number of messages.", type=click.INT, default=sys.maxsize, required=False
)
@click.option(
    "-m",
    "--match",
    metavar="<filter_expression>",
    help="Message filtering expression.",
    type=click.STRING,
    required=False,
)
@click.option("--last/--first", help="Start consuming from the earliest or latest offset in the topic.", default=False)
@click.option(
    "-a",
    "--avro",
    help="Set this flag if the topic contains avro data. This flag is mutually exclusive with the --binary flag",
    default=False,
    is_flag=True,
)
@click.option(
    "-b",
    "--binary",
    help="Set this flag if the topic contains binary data. Or the data should not be (de-)serialized. "
    "This flag is mutually exclusive with the --avro flag",
    default=False,
    is_flag=True,
)
@click.option(
    "-c",
    "--consumergroup",
    metavar="<consumer_group>",
    help="Consumer group to store the offset in.",
    type=click.STRING,
    autocompletion=list_consumergroups,
    default=None,
    required=False,
)
@click.option(
    "--preserve-order",
    help="Preserve the order of messages, regardless of their partition.",
    default=False,
    is_flag=True,
)
@click.option("--stdout", "write_to_stdout", help="Write messages to STDOUT.", default=False, is_flag=True)
@default_options
def consume(
    state: State,
    topic: str,
    from_context: str,
    number: int,
    match: str,
    last: bool,
    avro: bool,
    binary: bool,
    directory: str,
    consumergroup: str,
    preserve_order: bool,
    write_to_stdout: bool,
):
    """Consume messages from a topic.

    Read messages from a given topic in a given context. These messages can either be written
    to files in an automatically generated directory (default behavior), or to STDOUT.

    \b
    EXAMPLES:
    # Consume the first 10 messages from TOPIC in the current context and print them to STDOUT in order.
    esque consume --first -n 10 --preserve-order --stdout TOPIC

    \b
    # Consume <n> messages, starting from the 10th, from TOPIC in the <source_ctx> context and write them to files.
    esque consume --match "message.offset > 9" -n <n> TOPIC -f <source_ctx>

    \b
    # Copy source_topic in first context to destination_topic in second-context.
    esque consume -f first-context --stdout source_topic | esque produce -t second-context --stdin destination_topic
    """
    current_timestamp_milliseconds = int(round(time.time() * 1000))

    if binary and avro:
        raise ValueError("Cannot set data to be interpreted as binary AND avro.")

    if directory and write_to_stdout:
        raise ValueError("Cannot write to a directory and STDOUT, please pick one!")

    if not from_context:
        from_context = state.config.current_context
    state.config.context_switch(from_context)

    if topic not in map(attrgetter("name"), state.cluster.topic_controller.list_topics(get_topic_objects=False)):
        raise TopicDoesNotExistException(f"Topic {topic} does not exist!", -1)

    if not consumergroup:
        consumergroup = ESQUE_GROUP_ID
    if not directory:
        directory = Path() / "messages" / topic / str(current_timestamp_milliseconds)
    output_directory = Path(directory)

    if not write_to_stdout:
        click.echo(f"Creating directory {blue_bold(str(output_directory))} if it does not exist.")
        output_directory.mkdir(parents=True, exist_ok=True)
        click.echo(f"Start consuming from topic {blue_bold(topic)} in source context {blue_bold(from_context)}.")
    if preserve_order:
        partitions = [
            partition.partition_id for partition in state.cluster.topic_controller.get_cluster_topic(topic).partitions
        ]
        total_number_of_consumed_messages = consume_to_file_ordered(
            output_directory=output_directory,
            topic=topic,
            group_id=consumergroup,
            partitions=partitions,
            desired_message_count=number,
            avro=avro,
            match=match,
            last=last,
            write_to_stdout=write_to_stdout,
            binary=binary,
        )
    else:
        total_number_of_consumed_messages = consume_to_files(
            output_directory=output_directory,
            topic=topic,
            group_id=consumergroup,
            desired_message_count=number,
            avro=avro,
            match=match,
            last=last,
            write_to_stdout=write_to_stdout,
            binary=binary,
        )

    if not write_to_stdout:
        click.echo(f"Output generated to {blue_bold(str(output_directory))}")
        if total_number_of_consumed_messages == number or number == sys.maxsize:
            click.echo(blue_bold(str(total_number_of_consumed_messages)) + " messages consumed.")
        else:
            click.echo(
                "Only found "
                + bold(str(total_number_of_consumed_messages))
                + " messages in topic, out of "
                + blue_bold(str(number))
                + " required."
            )
