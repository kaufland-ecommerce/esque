import click

from esque.cli.autocomplete import list_consumergroups, list_contexts, list_topics
from esque.cli.helpers import ensure_approval
from esque.cli.options import State, default_options
from esque.cli.output import blue_bold, green_bold
from esque.cluster import Cluster
from esque.config import ESQUE_GROUP_ID
from esque.io.handlers import KafkaHandler
from esque.io.handlers.kafka import KafkaHandlerConfig
from esque.io.pipeline import PipelineBuilder
from esque.io.serializers import RawSerializer, RegistryAvroSerializer, StringSerializer
from esque.io.serializers.base import MessageSerializer
from esque.io.serializers.raw import RawSerializerConfig
from esque.io.serializers.registry_avro import RegistryAvroSerializerConfig
from esque.io.serializers.string import StringSerializerConfig
from esque.io.stream_decorators import event_counter, yield_only_matching_messages
from esque.resources.topic import Topic


@click.command("transfer")
@click.option(
    "-f",
    "--from-context",
    "from_context",
    metavar="<source_ctx>",
    help="Source context. If not provided, the current context will be used.",
    shell_complete=list_contexts,
    type=click.STRING,
    required=False,
)
@click.option(
    "--from-topic",
    "from_topic",
    metavar="<source_topic>",
    help="Source topic.",
    shell_complete=list_topics,
    type=click.STRING,
    required=True,
)
@click.option(
    "-t",
    "--to-context",
    "to_context",
    metavar="<destination_ctx>",
    help="Destination context. If not provided, the source context will be used.",
    type=click.STRING,
    shell_complete=list_contexts,
    required=False,
)
@click.option(
    "--to-topic",
    "to_topic",
    metavar="<destination_topic>",
    help="Destination topic.",
    shell_complete=list_topics,
    type=click.STRING,
    required=True,
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
@default_options
def transfer(
    state: State,
    from_topic: str,
    to_topic: str,
    from_context: str,
    to_context: str,
    number: int,
    last: bool,
    avro: bool,
    binary: bool,
    consumergroup: str,
    match: str = None,
):
    """Transfer messages between two topics.

    Read messages from the source topic in the source context and write them into the destination topic in the destination context.
    This function is shorthand for using a combination of `esque consume` and `esque produce`

    \b
    EXAMPLES:
    # Transfer the first 10 messages from TOPIC1 in the current context to TOPIC2 in context DSTCTX.
    esque transfer --first -n 10 --from-topic TOPIC1 --to-topic TOPIC2 --to-context DSTCTX

    \b
    # Transfer the first 10 messages from TOPIC1 in the context SRCCTX to TOPIC2 in context DSTCTX, assuming the messages are AVRO.
    esque transfer --first -n 10 --avro --from-topic TOPIC1 --from-context SRCCTX --to-topic TOPIC2 --to-context DSTCTX
    """
    if not from_context:
        from_context = state.config.current_context
    state.config.context_switch(from_context)

    if binary and avro:
        raise ValueError("Cannot set data to be interpreted as binary AND avro.")

    if not to_context:
        to_context = from_context

    if from_context == to_context and from_topic == to_topic:
        raise ValueError("Cannot transfer data to the same topic.")

    topic_controller = Cluster().topic_controller
    if not topic_controller.topic_exists(to_topic):
        if ensure_approval(f"Topic {to_topic!r} does not exist, do you want to create it?", no_verify=state.no_verify):
            topic_controller.create_topics([Topic(to_topic)])
        else:
            click.echo(click.style("Aborted!", bg="red"))
            return

    builder = PipelineBuilder()

    input_message_serializer = create_input_serializer(avro, binary, state)
    builder.with_input_message_serializer(input_message_serializer)

    input_handler = create_input_handler(consumergroup, from_context, from_topic)
    builder.with_input_handler(input_handler)

    output_message_serializer = create_output_serializer(avro, binary, to_topic, state)
    builder.with_output_message_serializer(output_message_serializer)

    output_handler = create_output_handler(to_context, to_topic)
    builder.with_output_handler(output_handler)

    if last:
        start = KafkaHandler.OFFSET_AFTER_LAST_MESSAGE
    else:
        start = KafkaHandler.OFFSET_AT_FIRST_MESSAGE

    builder.with_range(start=start, limit=number)

    if match:
        builder.with_stream_decorator(yield_only_matching_messages(match))

    counter, counter_decorator = event_counter()

    builder.with_stream_decorator(counter_decorator)

    pipeline = builder.build()
    pipeline.run_pipeline()
    click.echo(
        green_bold(str(counter.message_count))
        + " messages consumed from topic "
        + blue_bold(from_topic)
        + " in context "
        + blue_bold(from_context)
        + " and produced to topic "
        + blue_bold(to_topic)
        + " in context "
        + blue_bold(to_context)
        + "."
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
