import pathlib

import click

from esque.cli.autocomplete import list_contexts, list_topics
from esque.cli.helpers import ensure_approval
from esque.cli.options import State, default_options
from esque.cli.output import blue_bold, green_bold
from esque.cluster import Cluster
from esque.io.handlers import BaseHandler, KafkaHandler, PathHandler, PipeHandler
from esque.io.handlers.kafka import KafkaHandlerConfig
from esque.io.handlers.path import PathHandlerConfig
from esque.io.handlers.pipe import PipeHandlerConfig
from esque.io.pipeline import PipelineBuilder
from esque.io.serializers import JsonSerializer, RawSerializer, RegistryAvroSerializer, StringSerializer
from esque.io.serializers.base import MessageSerializer
from esque.io.serializers.json import JsonSerializerConfig
from esque.io.serializers.raw import RawSerializerConfig
from esque.io.serializers.registry_avro import RegistryAvroSerializerConfig
from esque.io.serializers.string import StringSerializerConfig
from esque.io.stream_decorators import MessageStream, yield_only_matching_messages
from esque.io.stream_events import StreamEvent
from esque.resources.topic import Topic


@click.command("produce")
@click.argument("topic", autocompletion=list_topics)
@click.option(
    "-d",
    "--directory",
    metavar="<directory>",
    help="Directory containing Kafka messages.",
    type=click.STRING,
    required=False,
)
@click.option(
    "-t",
    "--to",
    "to_context",
    metavar="<destination_ctx>",
    help="Destination context.",
    type=click.STRING,
    autocompletion=list_contexts,
    required=False,
)
@click.option(
    "-m",
    "--match",
    metavar="<filter_expresion>",
    help="Message filtering expression.",
    type=click.STRING,
    required=False,
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
    "--stdin", "read_from_stdin", help="Read messages from STDIN instead of a directory.", default=False, is_flag=True
)
@click.option(
    "-y",
    "--ignore-errors",
    "ignore_stdin_errors",
    help="Only when reading from STDIN. If JSON validation fails, write the malformed JSON as a string in message value"
    " (without key and specified partition assignment).",
    default=False,
    is_flag=True,
)
@default_options
def produce(
    state: State,
    topic: str,
    to_context: str,
    directory: str,
    avro: bool,
    binary: bool,
    match: str = None,
    read_from_stdin: bool = False,
    ignore_stdin_errors: bool = False,
):
    """Produce messages to a topic.

    Write messages to a given topic in a given context. These messages can come from either a directory <directory>
    containing files corresponding to the different partitions or from STDIN.

    \b
    EXAMPLES:
    # Write all messages from the files in <directory> to TOPIC in the <destination_ctx> context.
    esque produce -d <directory> -t <destination_ctx> TOPIC

    \b
    # Start environment in terminal to write messages to TOPIC in the <destination_ctx> context.
    esque produce --stdin -f <destination_ctx> -y TOPIC

    \b
    # Copy source_topic to destination_topic.
    esque consume -f first-context --stdout source_topic | esque produce -t second-context --stdin destination_topic
    """
    if not to_context:
        to_context = state.config.current_context
    state.config.context_switch(to_context)

    if not read_from_stdin:
        if not directory:
            raise ValueError("Need to provide directory if not reading from stdin.")
        else:
            directory = pathlib.Path(directory)
    elif avro:
        raise ValueError("Cannot read avro data from stdin. Use a directory instead.")

    if binary and avro:
        raise ValueError("Cannot set data to be interpreted as binary AND avro.")

    topic_controller = Cluster().topic_controller
    if not topic_controller.topic_exists(topic):
        if ensure_approval(f"Topic {topic!r} does not exist, do you want to create it?", no_verify=state.no_verify):
            topic_controller.create_topics([Topic(topic)])
        else:
            click.echo(click.style("Aborted!", bg="red"))
            return

    builder = PipelineBuilder()

    input_handler = create_input_handler(directory, read_from_stdin)
    builder.with_input_handler(input_handler)

    input_message_serializer = create_input_message_serializer(directory, avro, binary)
    builder.with_input_message_serializer(input_message_serializer)

    output_message_serializer = create_output_serializer(avro, binary, topic, state)
    builder.with_output_message_serializer(output_message_serializer)

    output_handler = create_output_handler(to_context, topic)
    builder.with_output_handler(output_handler)

    total_number_of_messages_produced = 0

    if match:
        builder.with_stream_decorator(yield_only_matching_messages(match))

    def counter_decorator(message_stream: MessageStream) -> MessageStream:
        nonlocal total_number_of_messages_produced
        for msg in message_stream:
            if not isinstance(msg, StreamEvent):
                total_number_of_messages_produced += 1
            yield msg

    builder.with_stream_decorator(counter_decorator)

    pipeline = builder.build()
    pipeline.run_pipeline()

    click.echo(
        green_bold(str(total_number_of_messages_produced))
        + " messages successfully produced to topic "
        + blue_bold(topic)
        + " in context "
        + blue_bold(to_context)
        + "."
    )


def create_output_handler(to_context: str, topic: str):
    output_handler = KafkaHandler(KafkaHandlerConfig(scheme="kafka", host=to_context, path=topic))
    return output_handler


def create_output_serializer(avro: bool, binary: bool, topic: str, state: State) -> MessageSerializer:
    if binary and avro:
        raise ValueError("Cannot set data to be interpreted as binary AND avro.")

    elif binary:
        key_serializer = RawSerializer(RawSerializerConfig(scheme="raw"))
        value_serializer = key_serializer

    elif avro:
        config = RegistryAvroSerializerConfig(scheme="reg-avro", schema_registry_uri=state.config.schema_registry)
        key_serializer = RegistryAvroSerializer(config.with_key_subject_for_topic(topic))
        value_serializer = RegistryAvroSerializer(config.with_value_subject_for_topic(topic))
    else:
        key_serializer = StringSerializer(StringSerializerConfig(scheme="str"))
        value_serializer = key_serializer

    message_serializer = MessageSerializer(key_serializer=key_serializer, value_serializer=value_serializer)
    return message_serializer


def create_input_handler(directory: pathlib.Path, read_from_stdin: bool) -> BaseHandler:
    if read_from_stdin:
        handler = PipeHandler(PipeHandlerConfig(scheme="pipe", host="stdin", path=""))
    else:
        if not directory:
            raise ValueError("Need to provide a directory to read from!")
        handler = PathHandler(PathHandlerConfig(scheme="path", host="", path=str(directory)))
        click.echo(f"Reading data from {blue_bold(str(directory))}.")
    return handler


def create_input_message_serializer(directory: pathlib.Path, avro: bool, binary: bool) -> MessageSerializer:
    if avro:
        serializer = RegistryAvroSerializer(
            RegistryAvroSerializerConfig(scheme="reg-avro", schema_registry_uri=f"path:///{directory}")
        )
    elif binary:
        serializer = RawSerializer(RawSerializerConfig(scheme="raw"))
    else:
        serializer = JsonSerializer(JsonSerializerConfig(scheme="json"))
    return MessageSerializer(key_serializer=serializer, value_serializer=serializer)
