import pathlib
import sys
import time
from contextlib import ExitStack
from pathlib import Path
from shutil import copyfile
from time import sleep

import click
import yaml
from click import version_option
from esque import __version__

from esque.cli.helpers import ensure_approval
from esque.cli.options import error_handler
from esque.cli.options import no_verify_option
from esque.cli.options import pass_state
from esque.cli.options import State
from esque.cli.output import blue_bold
from esque.cli.output import bold
from esque.cli.output import green_bold
from esque.cli.output import pretty
from esque.cli.output import pretty_new_topic_configs
from esque.cli.output import pretty_topic_diffs
from esque.cli.output import pretty_unchanged_topic_configs
from esque.clients.consumer import ConsumerFactory
from esque.clients.consumer import PingConsumer
from esque.clients.producer import PingProducer
from esque.clients.producer import ProducerFactory
from esque.cluster import Cluster
from esque.config import Config
from esque.config import config_dir
from esque.config import config_path
from esque.config import PING_GROUP_ID
from esque.config import PING_TOPIC
from esque.config import sample_config_path
from esque.controller.consumergroup_controller import ConsumerGroupController
from esque.errors import EndOfPartitionReachedException
from esque.errors import MessageEmptyException
from esque.messages.message import decode_message
from esque.resources.broker import Broker
from esque.resources.topic import copy_to_local
from esque.resources.topic import Topic


@click.group(help="esque - an operational kafka tool.", invoke_without_command=True)
@click.option("--recreate-config", is_flag=True, default=False, help="Overwrites the config with the sample config.")
@no_verify_option
@version_option(__version__)
@pass_state
def esque(state, recreate_config: bool):
    if recreate_config:
        config_dir().mkdir(exist_ok=True)
        if ensure_approval(f"Should the current config in {config_dir()} get replaced?", no_verify=state.no_verify):
            copyfile(sample_config_path().as_posix(), config_path())


@esque.group(help="Get a quick overview of different resources.")
def get():
    pass


@esque.group(help="Get detailed information about a resource.")
def describe():
    pass


@esque.group(help="Create a new instance of a resource.")
def create():
    pass


@esque.group(help="Delete a resource.")
def delete():
    pass


@esque.group(help="Edit a resource")
def edit():
    pass


# TODO: Figure out how to pass the state object
def list_topics(ctx, args, incomplete):
    cluster = Cluster()
    return [topic["name"] for topic in cluster.topic_controller.list_topics(search_string=incomplete)]


def list_contexts(ctx, args, incomplete):
    config = Config()
    return [context for context in config.available_contexts if context.startswith(incomplete)]


@esque.command("ctx", help="Switch clusters.")
@click.argument("context", required=False, default=None, autocompletion=list_contexts)
@error_handler
@pass_state
def ctx(state, context):
    if not context:
        for c in state.config.available_contexts:
            if c == state.config.current_context:
                click.echo(bold(c))
            else:
                click.echo(c)
    if context:
        state.config.context_switch(context)
        click.echo(f"Switched to context: {context}")


@create.command("topic")
@click.argument("topic-name", required=True)
@no_verify_option
@click.option("-l", "--like", help="Topic to use as template", required=False)
@error_handler
@pass_state
def create_topic(state: State, topic_name: str, like=None):
    if not ensure_approval("Are you sure?", no_verify=state.no_verify):
        click.echo("Aborted")
        return

    topic_controller = state.cluster.topic_controller
    if like:
        template_config = topic_controller.get_cluster_topic(like)
        topic = Topic(
            topic_name, template_config.num_partitions, template_config.replication_factor, template_config.config
        )
    else:
        topic = Topic(topic_name)
    topic_controller.create_topics([topic])
    click.echo(click.style(f"Topic with name '{topic.name}'' successfully created", fg="green"))


@edit.command("topic")
@click.argument("topic-name", required=True)
@error_handler
@pass_state
def edit_topic(state: State, topic_name: str):
    controller = state.cluster.topic_controller
    topic = state.cluster.topic_controller.get_cluster_topic(topic_name)
    new_conf = click.edit(topic.to_yaml(only_editable=True), extension=".yml")

    # edit process can be aborted, ex. in vim via :q!
    if new_conf is None:
        click.echo("Change aborted")
        return

    local_topic = copy_to_local(topic)
    local_topic.update_from_yaml(new_conf)
    diff = pretty_topic_diffs({topic_name: controller.diff_with_cluster(local_topic)})
    click.echo(diff)

    if ensure_approval("Are you sure?"):
        controller.alter_configs([local_topic])


@delete.command("topic")
@click.argument("topic-name", required=True, type=click.STRING, autocompletion=list_topics)
@no_verify_option
@error_handler
@pass_state
def delete_topic(state: State, topic_name: str):
    topic_controller = state.cluster.topic_controller
    if ensure_approval("Are you sure?", no_verify=state.no_verify):
        topic_controller.delete_topic(Topic(topic_name))

        assert topic_name not in (t.name for t in topic_controller.list_topics())

    click.echo(click.style(f"Topic with name '{topic_name}'' successfully deleted", fg="green"))


@esque.command("apply", help="Apply a configuration")
@click.option("-f", "--file", help="Config file path", required=True)
@no_verify_option
@error_handler
@pass_state
def apply(state: State, file: str):
    # Get topic data based on the YAML
    yaml_topic_configs = yaml.safe_load(open(file)).get("topics")
    yaml_topics = [Topic.from_dict(conf) for conf in yaml_topic_configs]
    yaml_topic_names = [t.name for t in yaml_topics]
    if not len(yaml_topic_names) == len(set(yaml_topic_names)):
        raise ValueError("Duplicate topic names in the YAML!")

    # Get topic data based on the cluster state
    topic_controller = state.cluster.topic_controller
    cluster_topics = topic_controller.list_topics(search_string="|".join(yaml_topic_names))
    cluster_topic_names = [t.name for t in cluster_topics]

    # Calculate changes
    to_create = [yaml_topic for yaml_topic in yaml_topics if yaml_topic.name not in cluster_topic_names]
    to_edit = [
        yaml_topic
        for yaml_topic in yaml_topics
        if yaml_topic not in to_create and topic_controller.diff_with_cluster(yaml_topic).has_changes
    ]
    to_edit_diffs = {t.name: topic_controller.diff_with_cluster(t) for t in to_edit}
    to_ignore = [yaml_topic for yaml_topic in yaml_topics if yaml_topic not in to_create and yaml_topic not in to_edit]

    # Sanity check - the 3 groups of topics should be complete and have no overlap
    assert (
        set(to_create).isdisjoint(set(to_edit))
        and set(to_create).isdisjoint(set(to_ignore))
        and set(to_edit).isdisjoint(set(to_ignore))
        and len(to_create) + len(to_edit) + len(to_ignore) == len(yaml_topics)
    )

    # Print diffs so the user can check
    click.echo(pretty_unchanged_topic_configs(to_ignore))
    click.echo(pretty_new_topic_configs(to_create))
    click.echo(pretty_topic_diffs(to_edit_diffs))

    # Check for actionable changes
    if len(to_edit) + len(to_create) == 0:
        click.echo("No changes detected, aborting")
        return

    # Warn users & abort when replication & num_partition changes are attempted
    if any(not diff.is_valid for _, diff in to_edit_diffs.items()):
        click.echo(
            "Changes to `replication_factor` and `num_partitions` can not be applied on already existing topics"
        )
        click.echo("Cancelling due to invalid changes")
        return

    # Get approval
    if not ensure_approval("Apply changes?", no_verify=state.no_verify):
        click.echo("Cancelling changes")
        return

    # apply changes
    topic_controller.create_topics(to_create)
    topic_controller.alter_configs(to_edit)

    # output confirmation
    changes = {"unchanged": len(to_ignore), "created": len(to_create), "changed": len(to_edit)}
    click.echo(click.style(pretty({"Successfully applied changes": changes}), fg="green"))


@describe.command("topic")
@click.argument("topic-name", required=True, type=click.STRING, autocompletion=list_topics)
@error_handler
@pass_state
def describe_topic(state, topic_name):
    topic = state.cluster.topic_controller.get_cluster_topic(topic_name)
    config = {"Config": topic.config}

    click.echo(bold(f"Topic: {green_bold(topic_name)}"))

    for partition in topic.partitions:
        click.echo(pretty({f"Partition {partition.partition_id}": partition.as_dict()}, break_lists=True))

    click.echo(pretty(config))


@get.command("offsets")
@click.argument("topic-name", required=False, type=click.STRING, autocompletion=list_topics)
@error_handler
@pass_state
def get_offsets(state, topic_name):
    # TODO: Gathering of all offsets takes super long
    topics = state.cluster.topic_controller.list_topics(search_string=topic_name)

    offsets = {topic.name: max(v for v in topic.offsets.values()) for topic in topics}

    click.echo(pretty(offsets))


@describe.command("broker")
@click.argument("broker-id", required=True)
@error_handler
@pass_state
def describe_broker(state, broker_id):
    broker = Broker.from_id(state.cluster, broker_id).describe()
    click.echo(pretty(broker, break_lists=True))


@describe.command("consumergroup")
@click.argument("consumer-id", required=False)
@click.option(
    "--all-partitions",
    help="List status for all topic partitions instead of just summarizing each topic.",
    default=False,
    is_flag=True,
)
@error_handler
@pass_state
def describe_consumergroup(state, consumer_id, all_partitions):
    consumer_group = ConsumerGroupController(state.cluster).get_consumergroup(consumer_id)
    consumer_group_desc = consumer_group.describe(verbose=all_partitions)

    click.echo(pretty(consumer_group_desc, break_lists=True))


@get.command("brokers")
@error_handler
@pass_state
def get_brokers(state):
    brokers = Broker.get_all(state.cluster)
    for broker in brokers:
        click.echo(f"{broker.broker_id}: {broker.host}:{broker.port}")


@get.command("consumergroups")
@error_handler
@pass_state
def get_consumergroups(state):
    groups = ConsumerGroupController(state.cluster).list_consumer_groups()
    for group in groups:
        click.echo(group)


@get.command("topics")
@click.argument("topic", required=False, type=click.STRING, autocompletion=list_topics)
@error_handler
@pass_state
def get_topics(state, topic):
    topics = state.cluster.topic_controller.list_topics(search_string=topic, get_topic_objects=False)
    for topic in topics:
        click.echo(topic.name)


@esque.command("consume", help="Consume messages of a topic from one environment to a file or STDOUT")
@click.argument("topic", required=True)
@click.option("-f", "--from", "from_context", help="Source Context", type=click.STRING, required=True)
@click.option("-n", "--numbers", help="Number of messages", type=click.INT, required=True)
@click.option("-m", "--match", help="Message filtering expression", type=click.STRING, required=False)
@click.option("--last/--first", help="Start consuming from the earliest or latest offset in the topic.", default=False)
@click.option("-a", "--avro", help="Set this flag if the topic contains avro data", default=False, is_flag=True)
@click.option(
    "-o",
    "--preserve-order",
    help="Preserve the order of messages, regardless of their partition",
    default=False,
    is_flag=True,
)
@click.option(
    "--stdout",
    "write_to_stdout",
    help="Write messages to STDOUT or to an automatically generated file.",
    default=False,
    is_flag=True,
)
@error_handler
@pass_state
def consume(
    state: State,
    topic: str,
    from_context: str,
    numbers: int,
    match: str,
    last: bool,
    avro: bool,
    preserve_order: bool,
    write_to_stdout: bool,
):
    current_timestamp_milliseconds = int(round(time.time() * 1000))
    unique_name = topic + "_" + str(current_timestamp_milliseconds)
    group_id = "group_for_" + unique_name
    directory_name = "message_" + unique_name
    working_dir = Path(directory_name)
    if not write_to_stdout:
        click.echo(f"Switching to context: {from_context}")
    state.config.context_switch(from_context)
    if not write_to_stdout:
        working_dir.mkdir(parents=True)
        click.echo("Creating directory " + blue_bold(working_dir.absolute().name))
        click.echo("Start consuming from topic " + blue_bold(topic) + " in source context " + blue_bold(from_context))
    if preserve_order:
        partitions = []
        for partition in state.cluster.topic_controller.get_cluster_topic(topic).partitions:
            partitions.append(partition.partition_id)
        total_number_of_consumed_messages = _consume_to_file_ordered(
            working_dir=working_dir,
            topic=topic,
            group_id=group_id,
            partitions=partitions,
            numbers=numbers,
            avro=avro,
            match=match,
            last=last,
            write_to_stdout=write_to_stdout,
        )
    else:
        total_number_of_consumed_messages = _consume_to_files(
            working_dir=working_dir,
            topic=topic,
            group_id=group_id,
            numbers=numbers,
            avro=avro,
            match=match,
            last=last,
            write_to_stdout=write_to_stdout,
        )

    if not write_to_stdout:
        click.echo("Output generated to " + blue_bold(directory_name))
        if total_number_of_consumed_messages == numbers:
            click.echo(blue_bold(str(total_number_of_consumed_messages)) + " messages consumed.")
        else:
            click.echo(
                "Found only "
                + bold(str(total_number_of_consumed_messages))
                + " messages in topic, out of "
                + blue_bold(str(numbers))
                + " required."
            )


def _consume_to_file_ordered(
    working_dir: pathlib.Path,
    topic: str,
    group_id: str,
    partitions: list,
    numbers: int,
    avro: bool,
    match: str,
    last: bool,
    write_to_stdout: bool = False,
) -> int:
    consumers = []
    factory = ConsumerFactory()
    for partition in partitions:
        consumer = factory.create_consumer(
            group_id=group_id + "_" + str(partition),
            topic_name=None,
            working_dir=working_dir,
            avro=avro,
            match=match,
            last=last,
        )
        consumer.assign_specific_partitions(topic, [partition])
        consumers.append(consumer)

    messages_by_partition = {}
    partitions_by_timestamp = {}
    total_number_of_messages = 0
    no_more_messages = False
    # get at least one message from each partition, or exclude those that don't have any messages
    for partition_counter in range(0, len(consumers)):
        try:
            message = consumers[partition_counter].consume_single_acceptable_message(timeout=10)
            decoded_message = decode_message(message)
        except (MessageEmptyException, EndOfPartitionReachedException):
            partitions.remove(partition_counter)
            if len(partitions) == 0:
                no_more_messages = True
        else:
            partitions_by_timestamp[decoded_message.timestamp] = decoded_message.partition
            if decoded_message.partition not in messages_by_partition:
                messages_by_partition[decoded_message.partition] = []
            messages_by_partition[decoded_message.partition].append(message)

    # in each iteration, take the earliest message from the map, output it and replace it with a new one (if available)
    # if not, remove the consumer and move to the next one
    with ExitStack() as stack:
        file_writer = consumers[0].get_file_writer(-1)
        stack.enter_context(file_writer)
        while total_number_of_messages < numbers and not no_more_messages:
            if len(partitions_by_timestamp) == 0:
                no_more_messages = True
            else:
                first_key = sorted(partitions_by_timestamp.keys())[0]
                partition = partitions_by_timestamp[first_key]

                message = messages_by_partition[partition].pop(0)
                consumers[0].output_consumed(message, file_writer if not write_to_stdout else None)
                del partitions_by_timestamp[first_key]
                total_number_of_messages += 1

                try:
                    message = consumers[partition].consume_single_acceptable_message(timeout=10)
                    decoded_message = decode_message(message)
                    partitions_by_timestamp[decoded_message.timestamp] = partition
                    messages_by_partition[partition].append(message)
                except (MessageEmptyException, EndOfPartitionReachedException):
                    partitions.remove(partition)
                    messages_by_partition.pop(partition, None)
                    if len(partitions) == 0:
                        no_more_messages = True

    return total_number_of_messages


def _consume_to_files(
    working_dir: pathlib.Path,
    topic: str,
    group_id: str,
    numbers: int,
    avro: bool,
    match: str,
    last: bool,
    write_to_stdout: bool = False,
) -> int:
    consumer = ConsumerFactory().create_consumer(
        group_id=group_id,
        topic_name=topic,
        working_dir=working_dir if not write_to_stdout else None,
        last=last,
        avro=avro,
        match=match,
    )
    number_consumed_messages = consumer.consume(int(numbers))

    return number_consumed_messages


@esque.command("produce", help="Produce messages from <directory> based on output from transfer command")
@click.argument("topic", required=True)
@click.option(
    "-d",
    "--directory",
    metavar="<directory>",
    help="Sets the directory that contains Kafka messages",
    type=click.STRING,
    required=False,
)
@click.option("-t", "--to", "to_context", help="Destination Context", type=click.STRING, required=True)
@click.option("-m", "--match", help="Message filtering expression", type=click.STRING, required=False)
@click.option("-a", "--avro", help="Set this flag if the topic contains avro data", default=False, is_flag=True)
@click.option(
    "-i",
    "--stdin",
    "read_from_stdin",
    help="Read messages from STDIN instead of a directory.",
    default=False,
    is_flag=True,
)
@click.option(
    "-y",
    "--ignore-errors",
    "ignore_stdin_errors",
    help="When reading from STDIN, use malformed strings as message values (ignore JSON).",
    default=False,
    is_flag=True,
)
@error_handler
@pass_state
def produce(
    state: State,
    topic: str,
    to_context: str,
    directory: str,
    avro: bool,
    match: str = None,
    read_from_stdin: bool = False,
    ignore_stdin_errors: bool = False,
):
    if directory is None and not read_from_stdin:
        click.echo("You have to provide the directory or use a --stdin flag")
    else:
        if directory is not None:
            working_dir = pathlib.Path(directory)
            if not working_dir.exists():
                click.echo("You have to provide an existing directory")
                exit(1)
        state.config.context_switch(to_context)
        if read_from_stdin and sys.stdin.isatty():
            click.echo(
                "Type the messages to produce, " + blue_bold("one per line") + ". End with " + blue_bold("CTRL+D")
            )
        elif read_from_stdin and not sys.stdin.isatty():
            click.echo("Reading messages from an external source, " + blue_bold("one per line"))
        else:
            click.echo(
                "Producing from directory "
                + directory
                + " to topic "
                + blue_bold(topic)
                + " in target context "
                + blue_bold(to_context)
            )
        producer = ProducerFactory().create_producer(
            topic_name=topic,
            working_dir=working_dir if not read_from_stdin else None,
            avro=avro,
            match=match,
            ignore_stdin_errors=ignore_stdin_errors,
        )
        total_number_of_messages_produced = producer.produce()
        click.echo(
            green_bold(str(total_number_of_messages_produced))
            + " messages successfully produced to context "
            + blue_bold(to_context)
            + " and topic "
            + blue_bold(topic)
            + "."
        )


@esque.command("ping", help="Tests the connection to the kafka cluster.")
@click.option("-t", "--times", help="Number of pings.", default=10)
@click.option("-w", "--wait", help="Seconds to wait between pings.", default=1)
@error_handler
@pass_state
def ping(state, times, wait):
    topic_controller = state.cluster.topic_controller
    deltas = []
    try:
        topic_controller.create_topics([Topic(PING_TOPIC)])

        producer = PingProducer()
        consumer = PingConsumer(PING_GROUP_ID, PING_TOPIC, True)

        click.echo(f"Ping with {state.cluster.bootstrap_servers}")

        for i in range(times):
            producer.produce(PING_TOPIC)
            _, delta = consumer.consume()
            deltas.append(delta)
            click.echo(f"m_seq={i} time={delta:.2f}ms")
            sleep(wait)
    except KeyboardInterrupt:
        pass
    finally:
        topic_controller.delete_topic(Topic(PING_TOPIC))
        click.echo("--- statistics ---")
        click.echo(f"{len(deltas)} messages sent/received")
        click.echo(f"min/avg/max = {min(deltas):.2f}/{(sum(deltas) / len(deltas)):.2f}/{max(deltas):.2f} ms")
