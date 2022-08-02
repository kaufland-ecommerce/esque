import datetime
import pathlib
from pathlib import Path
from typing import Optional

import click

from esque.cli.autocomplete import list_consumergroups, list_contexts, list_topics
from esque.cli.options import State, default_options
from esque.cli.output import blue_bold, bold
from esque.cluster import Cluster
from esque.config import ESQUE_GROUP_ID
from esque.io.handlers import KafkaHandler, PathHandler
from esque.io.handlers.kafka import KafkaHandlerConfig
from esque.io.handlers.path import PathHandlerConfig
from esque.io.handlers.pipe import PipeHandler, PipeHandlerConfig
from esque.io.pipeline import PipelineBuilder
from esque.io.serializers import JsonSerializer, RawSerializer, RegistryAvroSerializer, StringSerializer
from esque.io.serializers.base import MessageSerializer
from esque.io.serializers.json import JsonSerializerConfig
from esque.io.serializers.raw import RawSerializerConfig
from esque.io.serializers.registry_avro import RegistryAvroSerializerConfig
from esque.io.serializers.string import StringSerializerConfig
from esque.io.stream_decorators import event_counter, yield_messages_sorted_by_timestamp, yield_only_matching_messages


@click.command("consume")
@click.argument("topic", shell_complete=list_topics)
@click.option(
    "-d", "--directory", metavar="<directory>", help="Sets the directory to write the messages to.", type=click.STRING
)
@click.option(
    "-f",
    "--from",
    "from_context",
    metavar="<source_ctx>",
    help="Source context. If not provided, the current context will be used.",
    shell_complete=list_contexts,
    type=click.STRING,
    required=False,
)
@click.option(
    "-n", "--number", metavar="<n>", help="Number of messages.", type=click.INT, default=None, required=False
)
@click.option(
    "-m",
    "--match",
    metavar="<filter_expression>",
    help="Message filtering expression.",
    type=click.STRING,
    required=False,
)
@click.option(
    "--last/--first",
    help="Start consuming from the earliest or latest offset in the topic."
    "Latest means at the end of the topic _not including_ the last message(s),"
    "so if no new data is coming in nothing will be consumed.",
    default=False,
)
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
    shell_complete=list_consumergroups,
    default=None,
    required=False,
)
@click.option(
    "--preserve-order",
    help="Preserve the order of messages, regardless of their partition. "
    "Order is determined by timestamp and this feature assumes message timestamps are monotonically increasing "
    "within each partition. Will cause the consumer to stop at temporary ends which means it will ignore new messages.",
    default=False,
    is_flag=True,
)
@click.option("--stdout", "write_to_stdout", help="Write messages to STDOUT.", default=False, is_flag=True)
@click.option(
    "-p",
    "--pretty-print",
    help="Use multiple lines to represent each kafka message instead of putting every JSON object into a single "
    "line. Only has an effect when consuming to stdout.",
    default=False,
    is_flag=True,
)
@default_options
def consume(
    state: State,
    topic: str,
    from_context: str,
    number: Optional[int],
    match: str,
    last: bool,
    avro: bool,
    binary: bool,
    directory: str,
    consumergroup: str,
    preserve_order: bool,
    write_to_stdout: bool,
    pretty_print: bool,
):
    """Consume messages from a topic.

    Read messages from a given topic in a given context. These messages can either be written
    to files in an automatically generated directory (default behavior), or to STDOUT.

    If writing to STDOUT, then data will be represented as a JSON object with the message key and the message value
    always being a string.
    With the --avro option, those strings are JSON serialized objects.
    With the --binary option those strings contain the base64 encoded binary data.
    Without any of the two options, the data in the messages is treated utf-8 encoded strings and will be used as-is.

    \b
    EXAMPLES:
    # Consume the first 10 messages from TOPIC in the current context and print them to STDOUT in order.
    esque consume --first -n 10 --preserve-order --pretty-print --stdout TOPIC

    \b
    # Consume <n> messages, starting from the 10th, from TOPIC in the <source_ctx> context and write them to files.
    esque consume --match "message.offset > 9" -n <n> TOPIC -f <source_ctx>

    \b
    # Extract json objects from keys
    esque consume --stdout --avro TOPIC | jq '.key | fromjson'

    \b
    # Extract binary data from keys (depending on the data this could mess up your console)
    esque consume --stdout --binary TOPIC | jq '.key | @base64d'
    """
    if not from_context:
        from_context = state.config.current_context
    state.config.context_switch(from_context)

    if not write_to_stdout and not directory:
        directory = Path() / "messages" / topic / datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    if binary and avro:
        raise ValueError("Cannot set data to be interpreted as binary AND avro.")

    builder = PipelineBuilder()

    input_message_serializer = create_input_serializer(avro, binary, state)
    builder.with_input_message_serializer(input_message_serializer)

    input_handler = create_input_handler(consumergroup, from_context, topic)
    builder.with_input_handler(input_handler)

    output_handler = create_output_handler(directory, write_to_stdout, binary, pretty_print)
    builder.with_output_handler(output_handler)

    output_message_serializer = create_output_message_serializer(write_to_stdout, directory, avro, binary)
    builder.with_output_message_serializer(output_message_serializer)

    if last:
        start = KafkaHandler.OFFSET_AFTER_LAST_MESSAGE
    else:
        start = KafkaHandler.OFFSET_AT_FIRST_MESSAGE

    builder.with_range(start=start, limit=number)

    if preserve_order:
        topic_data = Cluster().topic_controller.get_cluster_topic(topic, retrieve_partition_watermarks=False)
        builder.with_stream_decorator(yield_messages_sorted_by_timestamp(len(topic_data.partitions)))

    if match:
        builder.with_stream_decorator(yield_only_matching_messages(match))

    counter, counter_decorator = event_counter()

    builder.with_stream_decorator(counter_decorator)

    pipeline = builder.build()
    pipeline.run_pipeline()

    if not write_to_stdout:
        if counter.message_count == number:
            click.echo(blue_bold(str(counter.message_count)) + " messages consumed.")
        else:
            click.echo(
                "Only found "
                + bold(str(counter.message_count))
                + " messages in topic, out of "
                + blue_bold(str(number))
                + " required."
            )


def create_input_handler(consumergroup, from_context, topic):
    if not consumergroup:
        consumergroup = ESQUE_GROUP_ID
    input_handler = KafkaHandler(
        KafkaHandlerConfig(scheme="kafka", host=from_context, path=topic, consumer_group_id=consumergroup)
    )
    return input_handler


def create_input_serializer(avro, binary, state):
    if binary:
        input_serializer = RawSerializer(RawSerializerConfig(scheme="raw"))
    elif avro:
        input_serializer = RegistryAvroSerializer(
            RegistryAvroSerializerConfig(scheme="reg-avro", schema_registry_uri=state.config.schema_registry)
        )
    else:
        input_serializer = StringSerializer(StringSerializerConfig(scheme="str"))
    input_message_serializer = MessageSerializer(key_serializer=input_serializer, value_serializer=input_serializer)
    return input_message_serializer


def create_output_handler(directory: pathlib.Path, write_to_stdout: bool, binary: bool, pretty_print: bool):
    if directory and write_to_stdout:
        raise ValueError("Cannot write to a directory and STDOUT, please pick one!")
    elif write_to_stdout:
        encoding = "base64" if binary else "utf-8"
        pretty_print = "1" if pretty_print else ""
        output_handler = PipeHandler(
            PipeHandlerConfig(
                scheme="pipe",
                host="stdout",
                path="",
                key_encoding=encoding,
                value_encoding=encoding,
                pretty_print=pretty_print,
            )
        )
    else:
        output_handler = PathHandler(PathHandlerConfig(scheme="path", host="", path=str(directory)))
        click.echo(f"Writing data to {blue_bold(str(directory))}.")
    return output_handler


def create_output_message_serializer(
    write_to_stdout: bool, directory: pathlib.Path, avro: bool, binary: bool
) -> MessageSerializer:
    if avro and write_to_stdout:
        serializer = JsonSerializer(JsonSerializerConfig(scheme="json"))
    elif avro and not write_to_stdout:
        serializer = RegistryAvroSerializer(
            RegistryAvroSerializerConfig(scheme="reg-avro", schema_registry_uri=f"path:///{directory}")
        )
    elif binary:
        serializer = RawSerializer(RawSerializerConfig(scheme="raw"))
    else:
        serializer = StringSerializer(StringSerializerConfig(scheme="str"))
    return MessageSerializer(key_serializer=serializer, value_serializer=serializer)
