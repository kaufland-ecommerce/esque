import pathlib
import time
from pathlib import Path
from shutil import copyfile
from time import sleep

import click
import yaml
from click import MissingParameter, UsageError, version_option

from esque import __version__
from esque.cli.helpers import HandleFileOnFinished, ensure_approval, isatty
from esque.cli.options import State, error_handler, no_verify_option, output_format_option, pass_state
from esque.cli.output import (
    blue_bold,
    bold,
    format_output,
    green_bold,
    pretty,
    pretty_new_topic_configs,
    pretty_topic_diffs,
    pretty_unchanged_topic_configs,
    red_bold,
)
from esque.clients.consumer import AvroFileConsumer, FileConsumer, PingConsumer
from esque.clients.producer import AvroFileProducer, FileProducer, PingProducer
from esque.cluster import Cluster
from esque.config import PING_GROUP_ID, PING_TOPIC, Config, config_dir, config_path, sample_config_path
from esque.controller.consumergroup_controller import ConsumerGroupController
from esque.resources.broker import Broker
from esque.resources.topic import Topic, copy_to_local


@click.group(help="esque - an operational kafka tool.", invoke_without_command=True)
@click.option("--recreate-config", is_flag=True, default=False, help="Overwrites the config with the sample config.")
@no_verify_option
@version_option(__version__)
@pass_state
def esque(state: State, recreate_config: bool):
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


@esque.group(help="Edit a resource or your esque config")
def edit():
    pass


# TODO: Figure out how to pass the state object
def list_topics(ctx, args, incomplete):
    cluster = Cluster()
    return [topic["name"] for topic in cluster.topic_controller.list_topics(search_string=incomplete)]


def list_contexts(ctx, args, incomplete):
    config = Config()
    return [context for context in config.available_contexts if context.startswith(incomplete)]


def fallback_to_stdin(ctx, param, value):
    stdin = click.get_text_stream("stdin")
    if not value and not isatty(stdin):
        stdin_arg = stdin.readline().strip()
    else:
        stdin_arg = value
    if not stdin_arg:
        raise MissingParameter("No value specified")

    return stdin_arg


@esque.command("ctx", help="Switch clusters.")
@click.argument("context", required=False, default=None, autocompletion=list_contexts)
@error_handler
@pass_state
def ctx(state: State, context: str):
    if not context:
        for c in state.config.available_contexts:
            if c == state.config.current_context:
                click.echo(bold(c))
            else:
                click.echo(c)
    if context:
        state.config.context_switch(context)


@create.command("topic")
@click.argument("topic-name", callback=fallback_to_stdin, required=False)
@click.option("-l", "--like", help="Topic to use as template", required=False)
@error_handler
@no_verify_option
@pass_state
def create_topic(state: State, topic_name: str, like: str):
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
@pass_state
@error_handler
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
@click.argument(
    "topic-name", callback=fallback_to_stdin, required=False, type=click.STRING, autocompletion=list_topics
)
@no_verify_option
@error_handler
@pass_state
def delete_topic(state: State, topic_name: str):
    topic_controller = state.cluster.topic_controller
    if ensure_approval("Are you sure?", no_verify=state.no_verify):
        topic_controller.delete_topic(Topic(topic_name))

        assert topic_name not in (t.name for t in topic_controller.list_topics(get_topic_objects=False))

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
        raise UsageError("Duplicate topic names in the YAML!")

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
@click.argument(
    "topic-name", callback=fallback_to_stdin, required=False, type=click.STRING, autocompletion=list_topics
)
@click.option(
    "--consumers",
    "-C",
    required=False,
    is_flag=True,
    default=False,
    help=f"Will output the consumergroups reading from this topic. "
    f"{red_bold('Beware! This can be a really expensive operation.')}",
)
@output_format_option
@error_handler
@pass_state
def describe_topic(state: State, topic_name: str, consumers: bool, output_format: str):
    topic = state.cluster.topic_controller.get_cluster_topic(topic_name)

    output_dict = {
        "topic": topic_name,
        "partitions": [partition.as_dict() for partition in topic.partitions],
        "config": topic.config,
    }

    if consumers:
        consumergroup_controller = ConsumerGroupController(state.cluster)
        groups = consumergroup_controller.list_consumer_groups()

        consumergroups = [
            group_name
            for group_name in groups
            if topic_name in consumergroup_controller.get_consumergroup(group_name).topics
        ]

        output_dict["consumergroups"] = consumergroups
    click.echo(format_output(output_dict, output_format))


@get.command("offsets")
@click.option("-t", "--topic-name", required=False, type=click.STRING, autocompletion=list_topics)
@output_format_option
@error_handler
@pass_state
def get_offsets(state: State, topic_name: str, output_format: str):
    # TODO: Gathering of all offsets takes super long
    topics = state.cluster.topic_controller.list_topics(search_string=topic_name)

    offsets = {topic.name: max(v for v in topic.offsets.values()) for topic in topics}

    click.echo(format_output(offsets, output_format))


@describe.command("broker")
@click.argument("broker-id", callback=fallback_to_stdin, required=False)
@error_handler
@pass_state
@output_format_option
def describe_broker(state: State, broker_id: str, output_format: str):
    broker = Broker.from_id(state.cluster, broker_id).describe()
    click.echo(format_output(broker, output_format))


@describe.command("consumergroup")
@click.option("-c", "--consumer-id", required=False)
@click.option(
    "--all-partitions",
    help="List status for all topic partitions instead of just summarizing each topic.",
    default=False,
    is_flag=True,
)
@error_handler
@pass_state
@output_format_option
def describe_consumergroup(state: State, consumer_id: str, all_partitions: bool, output_format: str):
    consumer_group = ConsumerGroupController(state.cluster).get_consumergroup(consumer_id)
    consumer_group_desc = consumer_group.describe(verbose=all_partitions)

    click.echo(format_output(consumer_group_desc, output_format))


@get.command("brokers")
@error_handler
@pass_state
@output_format_option
def get_brokers(state: State, output_format: str):
    brokers = Broker.get_all(state.cluster)
    broker_ids_and_hosts = [f"{broker.broker_id}: {broker.host}:{broker.port}" for broker in brokers]
    click.echo(format_output(broker_ids_and_hosts, output_format))


@get.command("consumergroups")
@error_handler
@pass_state
@output_format_option
def get_consumergroups(state: State, output_format: str):
    groups = ConsumerGroupController(state.cluster).list_consumer_groups()
    click.echo(format_output(groups, output_format))


@get.command("topics")
@click.option("-p", "--prefix", type=click.STRING, autocompletion=list_topics, required=False)
@output_format_option
@error_handler
@pass_state
def get_topics(state: State, prefix: str, output_format: str):
    topics = state.cluster.topic_controller.list_topics(search_string=prefix, get_topic_objects=False)
    topic_names = [topic.name for topic in topics]
    click.echo(format_output(topic_names, output_format))


@esque.command("transfer", help="Transfer messages of a topic from one environment to another.")
@click.argument("topic", required=True)
@click.option("-f", "--from", "from_context", help="Source Context", type=click.STRING, required=True)
@click.option("-t", "--to", "to_context", help="Destination context", type=click.STRING, required=True)
@click.option("-n", "--numbers", help="Number of messages", type=click.INT, required=True)
@click.option("-m", "--match", help="Match expression", type=click.STRING, required=False)
@click.option("--last/--first", help="Start consuming from the earliest or latest offset in the topic.", default=False)
@click.option("-a", "--avro", help="Set this flag if the topic contains avro data", default=False, is_flag=True)
@click.option(
    "-k",
    "--keep",
    "keep_file",
    help="Set this flag if the file with consumed messages should be kept.",
    default=False,
    is_flag=True,
)
@error_handler
@pass_state
def transfer(
    state: State,
    topic: str,
    from_context: str,
    to_context: str,
    numbers: int,
    match: str,
    last: bool,
    avro: bool,
    keep_file: bool,
):
    current_timestamp_milliseconds = int(round(time.time() * 1000))
    unique_name = topic + "_" + str(current_timestamp_milliseconds)
    group_id = "group_for_" + unique_name
    directory_name = "message_" + unique_name
    base_dir = Path(directory_name)
    state.config.context_switch(from_context)

    with HandleFileOnFinished(base_dir, keep_file) as working_dir:
        number_consumed_messages = _consume_to_files(
            working_dir, topic, group_id, from_context, numbers, avro, match, last
        )

        if number_consumed_messages == 0:
            click.echo(click.style("Execution stopped, because no messages consumed.", fg="red"))
            click.echo(bold("Possible reasons: The topic is empty or the starting offset was set too high."))
            return

        click.echo("\nReady to produce to context " + blue_bold(to_context) + " and target topic " + blue_bold(topic))

        if not ensure_approval("Do you want to proceed?\n", no_verify=state.no_verify):
            return

        state.config.context_switch(to_context)
        _produce_from_files(topic, to_context, working_dir, avro)


@esque.command("produce", help="Produce messages from <directory> based on output from transfer command")
@click.argument("topic", required=True)
@click.option(
    "-d",
    "--directory",
    metavar="<directory>",
    help="Sets the directory that contains Kafka messages",
    type=click.STRING,
    required=True,
)
@click.option("-a", "--avro", help="Set this flag if the topic contains avro data", default=False, is_flag=True)
@error_handler
@pass_state
def produce(state: State, topic: str, directory: str, avro: bool):
    _produce_from_files(topic, state.config.current_context, Path(directory), avro)


def _produce_from_files(topic: str, to_context: str, working_dir: pathlib.Path, avro: bool):
    if avro:
        producer = AvroFileProducer(working_dir)
    else:
        producer = FileProducer(working_dir)
    click.echo("\nStart producing to topic " + blue_bold(topic) + " in target context " + blue_bold(to_context))
    number_produced_messages = producer.produce(topic)
    click.echo(
        green_bold(str(number_produced_messages))
        + " messages successfully produced to context "
        + green_bold(to_context)
        + " and topic "
        + green_bold(topic)
        + "."
    )


def _consume_to_files(
    working_dir: pathlib.Path,
    topic: str,
    group_id: str,
    from_context: str,
    numbers: int,
    avro: bool,
    match: str,
    last: bool,
) -> int:
    if avro:
        consumer = AvroFileConsumer(group_id, topic, working_dir, last)
    else:
        consumer = FileConsumer(group_id, topic, working_dir, last)
    click.echo("\nStart consuming from topic " + blue_bold(topic) + " in source context " + blue_bold(from_context))
    number_consumed_messages = consumer.consume(int(numbers))
    click.echo(blue_bold(str(number_consumed_messages)) + " messages consumed.")

    return number_consumed_messages


@esque.command("ping", help="Tests the connection to the kafka cluster.")
@click.option("-t", "--times", help="Number of pings.", default=10)
@click.option("-w", "--wait", help="Seconds to wait between pings.", default=1)
@error_handler
@pass_state
def ping(state: State, times: int, wait: int):
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


@edit.command("config", help="Edit your esque config file.")
@error_handler
def edit_config():
    click.edit(filename=config_path().as_posix())
