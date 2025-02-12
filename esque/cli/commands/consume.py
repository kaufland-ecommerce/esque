import dataclasses
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
from esque.io.serializers import (
    JsonSerializer,
    ProtoSerializer,
    RawSerializer,
    RegistryAvroSerializer,
    StringSerializer,
)
from esque.io.serializers.base import MessageSerializer
from esque.io.serializers.json import JsonSerializerConfig
from esque.io.serializers.proto import ProtoSerializerConfig
from esque.io.serializers.raw import RawSerializerConfig
from esque.io.serializers.registry_avro import RegistryAvroSerializerConfig
from esque.io.serializers.string import StringSerializerConfig
from esque.io.serializers.struct import StructSerializer, StructSerializerConfig
from esque.io.stream_decorators import event_counter, yield_messages_sorted_by_timestamp, yield_only_matching_messages


@dataclasses.dataclass()
class SerializationConfig:
    serializer: str
    struct_format: str
    proto_key: str
    protoc_py_path: str
    protoc_module_name: str
    protoc_class_name: str


@click.command("consume", context_settings={"help_option_names": ["-h", "--help"]})
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
@click.option("--key-struct-format",
              help=" convert binary (packed) data into string. an example can be find here: https://docs.python.org/3/library/struct.html",
              type=str)
@click.option("--val-struct-format", help=" convert binary (packed) data into string", type=str)
@click.option(
    "-k",
    "--key-serializer",
    type=click.Choice(["str", "binary", "avro", "proto", "struct"], case_sensitive=False),
    help="Specify deserialization for keys. if you choose avro or binary value will also be set the same unless you choose differently.",
    default=None,
)
@click.option(
    "-s",
    "--val-serializer",
    type=click.Choice(["str", "binary", "avro", "proto", "struct"], case_sensitive=False),
    help="Specify deserialization for keys. if you choose avro or binary key will also be set the same unless you choose differently.",
    default=None,
)
@click.option(
    "--key-proto-key",
    type=click.STRING,
    help="proto key in configuration if you want to deserialize proto by anything other than topic name."
         " by default if -s is set to proto we set proto-key as topic name but this can be overwritten by this key"
         "this is used for key part of the message",
)
@click.option(
    "--key-protoc-py-path",
    type=click.STRING,
    help="compiled protobuf message path. this is used for key part of the message",
)
@click.option(
    "--key-protoc-module-name",
    type=click.STRING,
    help="module name for compiled protobuf message path. for example api.hi_pb2 if package name is api and file name is hi_pb2.py"
         "this is used for key part of the message",
)
@click.option(
    "--key-protoc-class-name",
    type=click.STRING,
    help="class name of message. this is used for key part of the message",
)
@click.option(
    "--val-proto-key",
    type=click.STRING,
    help="proto key in configuration if you want to deserialize proto by anything other than topic name."
         " by default if -s is set to proto we set proto-key as topic name but this can be overwritten by this key"
         "this is used for value part of the message",
)
@click.option(
    "--val-protoc-py-path",
    type=click.STRING,
    help="compiled protobuf message path. this is used for value part of the message",
)
@click.option(
    "--val-protoc-module-name",
    type=click.STRING,
    help="module name for compiled protobuf message path. for example api.hi_pb2 if package name is api and file name is hi_pb2.py"
         "this is used for value part of the message",
)
@click.option(
    "--val-protoc-class-name",
    type=click.STRING,
    help="class name of message. this is used for key part of the message",
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
@click.option("--stdout", "write_to_stdout", help="Write messages to STDOUT.", default=True, is_flag=True)
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
        key_struct_format: str,
        val_struct_format: str,
        key_serializer: str,
        val_serializer: str,
        directory: str,
        consumergroup: str,
        preserve_order: bool,
        write_to_stdout: bool,
        pretty_print: bool,
        key_proto_key: str,
        key_protoc_py_path: str,
        key_protoc_module_name: str,
        key_protoc_class_name: str,
        val_proto_key: str,
        val_protoc_py_path: str,
        val_protoc_module_name: str,
        val_protoc_class_name: str

):
    """Consume messages from a topic.

    Read messages from a given topic in a given context. These messages can either be written
    to files in an automatically generated directory, or to STDOUT((default behavior)).

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
    esque consume --stdout -s avro TOPIC | jq '.key | fromjson'

    \b
    # Extract binary data from keys (depending on the data this could mess up your console)
    esque consume --stdout -s binary TOPIC | jq '.key | @base64d'

    \b
    # Extract protobuf data from topic
    esque consume --stdout -s proto TOPIC | jq

    \b
    # Extract protobuf data from topic using specific proto-key
    esque consume --stdout -s proto --proto-key=topic-api-v2 TOPIC | jq

    \b
    # Extract protobuf data from topic using specific customised protobuf config
    esque consume --stdout -s proto --protoc_py_path=path --protoc_module_name=api.module_name --protoc_class_name=ModuleClass  TOPIC
    """
    if not from_context:
        from_context = state.config.current_context
    state.config.context_switch(from_context)

    if not write_to_stdout and not directory:
        directory = Path() / "messages" / topic / datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    builder = PipelineBuilder()

    key_config = SerializationConfig(
        serializer=key_serializer,
        struct_format=key_struct_format,
        proto_key=key_proto_key,
        protoc_py_path=key_protoc_py_path,
        protoc_module_name=key_protoc_module_name,
        protoc_class_name=key_protoc_class_name
    )

    val_config = SerializationConfig(
        serializer=val_serializer,
        struct_format=val_struct_format,
        proto_key=val_proto_key,
        protoc_py_path=val_protoc_py_path,
        protoc_module_name=val_protoc_module_name,
        protoc_class_name=val_protoc_class_name,
    )
    input_message_serializer = create_input_serializer(state, topic, key_config, val_config)
    builder.with_input_message_serializer(input_message_serializer)

    input_handler = create_input_handler(consumergroup, from_context, topic)
    builder.with_input_handler(input_handler)

    output_handler = create_output_handler(directory, write_to_stdout, key_serializer, val_serializer, pretty_print)
    builder.with_output_handler(output_handler)

    output_message_serializer = create_output_message_serializer(
        write_to_stdout, directory, key_serializer, val_serializer
    )
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


def create_input_serializer(state, topic, key_config, val_config):
    return MessageSerializer(
        key_serializer=create_serializer(state, topic, key_config),
        value_serializer=create_serializer(state, topic, val_config),
    )


def create_serializer(state: State, topic: str, config: SerializationConfig):
    if config.serializer == "str":
        return StringSerializer(StringSerializerConfig(scheme="str"))
    elif config.serializer == "avro":
        return RegistryAvroSerializer(
            RegistryAvroSerializerConfig(scheme="reg-avro", schema_registry_uri=state.config.schema_registry)
        )
    elif config.serializer == "binary":
        return RawSerializer(RawSerializerConfig(scheme="raw"))
    elif config.serializer == "proto":
        protoc_py_path = config.protoc_py_path
        module_name = config.protoc_module_name
        class_name = config.protoc_class_name
        if topic in state.config.proto or config.proto_key in state.config.proto:
            proto_cfg = state.config.proto[config.proto_key or topic]
            protoc_py_path = protoc_py_path or proto_cfg.get("protoc_py_path")
            module_name = module_name or proto_cfg.get("module_name")
            class_name = class_name or proto_cfg.get("class_name")
        if protoc_py_path is None or module_name is None or class_name is None:
            raise ValueError("protobuf configuration not found " )
        return ProtoSerializer(
            ProtoSerializerConfig(
                scheme="proto",
                protoc_py_path=protoc_py_path,
                module_name=module_name,
                class_name=class_name,
            )
        )
    elif config.serializer == "struct":
        return StructSerializer(StructSerializerConfig(scheme="struct", deserializer_struct_format=config.struct_format))
    raise ValueError("serializer " + config.serializer + " not found")


def create_output_handler(
        directory: pathlib.Path, write_to_stdout: bool, key_serializer, val_serializer: str, pretty_print: bool
):
    if directory and write_to_stdout:
        raise ValueError("Cannot write to a directory and STDOUT, please pick one!")
    elif write_to_stdout:
        return PipeHandler(
            PipeHandlerConfig(
                scheme="pipe",
                host="stdout",
                path="",
                key_encoding="base64" if key_serializer == "binary" else "utf-8",
                value_encoding="base64" if val_serializer == "binary" else "utf-8",
                pretty_print="1" if pretty_print else "",
            )
        )
    else:
        output_handler = PathHandler(PathHandlerConfig(scheme="path", host="", path=str(directory)))
        click.echo(f"Writing data to {blue_bold(str(directory))}.")
    return output_handler


def create_output_message_serializer(
        write_to_stdout: bool, directory: pathlib.Path, key_serializer, val_serializer: str
) -> MessageSerializer:
    def get_serializer_for_stdout(serializer):
        if serializer == "str":
            return StringSerializer(StringSerializerConfig(scheme="str"))
        if serializer == "avro" or serializer == "proto":
            return JsonSerializer(JsonSerializerConfig(scheme="json"))
        return RawSerializer(RawSerializerConfig(scheme="raw"))

    actual_key_serializer = get_serializer_for_stdout(key_serializer)
    actual_val_serializer = get_serializer_for_stdout(val_serializer)
    if not write_to_stdout and (key_serializer == "avro" or val_serializer == "avro"):
        actual_key_serializer = actual_val_serializer = RegistryAvroSerializer(
            RegistryAvroSerializerConfig(scheme="reg-avro", schema_registry_uri=f"path:///{directory}")
        )
    return MessageSerializer(key_serializer=actual_key_serializer, value_serializer=actual_val_serializer)
