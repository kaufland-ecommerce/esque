import shutil
import time
from pathlib import Path
from time import sleep

import click
import yaml
from click import version_option

from esque.__version__ import __version__
from esque.broker import Broker
from esque.cli.helpers import ensure_approval
from esque.cli.options import State, no_verify_option, pass_state
from esque.cli.output import (
    bold,
    pretty,
    pretty_topic_diffs,
    get_output_new_topics,
    blue_bold,
    green_bold,
)
from esque.clients import (
    FileConsumer,
    FileProducer,
    AvroFileProducer,
    AvroFileConsumer,
    PingConsumer,
    PingProducer,
)
from esque.cluster import Cluster
from esque.config import PING_TOPIC, Config, PING_GROUP_ID
from esque.consumergroup import ConsumerGroupController
from esque.errors import (
    ConsumerGroupDoesNotExistException,
    ContextNotDefinedException,
    TopicAlreadyExistsException,
    DeleteOnException,
)
from esque.topic import TopicController


@click.group(help="(Kafka-)esque.")
@version_option(__version__)
def esque():
    pass


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
    return [
        topic["name"]
        for topic in TopicController(cluster).list_topics(search_string=incomplete)
    ]


def list_contexts(ctx, args, incomplete):
    config = Config()
    return [
        context
        for context in config.available_contexts
        if context.startswith(incomplete)
    ]


@edit.command("topic")
@click.argument("topic-name", required=True)
@pass_state
def edit_topic(state: State, topic_name: str):
    controller = TopicController(state.cluster)
    topic = TopicController(state.cluster).get_topic(topic_name)
    new_conf = click.edit(topic.to_yaml())
    topic.from_yaml(new_conf)
    diff = pretty_topic_diffs({topic_name: topic.config_diff()})
    click.echo(diff)
    if ensure_approval("Are you sure?"):
        controller.alter_configs([topic])


@esque.command("ctx", help="Switch clusters.")
@click.argument("context", required=False, default=None, autocompletion=list_contexts)
@pass_state
def ctx(state, context):
    if not context:
        for c in state.config.available_contexts:
            if c == state.config.current_context:
                click.echo(bold(c))
            else:
                click.echo(c)
    if context:
        try:
            state.config.context_switch(context)
        except ContextNotDefinedException:
            click.echo(f"Context {context} does not exist")


@create.command("topic")
@click.argument("topic-name", required=True)
@no_verify_option
@pass_state
def create_topic(state: State, topic_name: str):
    if ensure_approval("Are you sure?", no_verify=state.no_verify):
        topic_controller = TopicController(state.cluster)
        TopicController(state.cluster).create_topics(
            [(topic_controller.get_topic(topic_name))]
        )


@esque.command("apply", help="Apply a configuration")
@click.option("-f", "--file", help="Config file path", required=True)
@pass_state
def apply(state: State, file: str):
    topic_controller = TopicController(state.cluster)
    yaml_data = yaml.safe_load(open(file))
    topic_configs = yaml_data.get("topics")
    topics = []
    for topic_config in topic_configs:
        topics.append(
            topic_controller.get_topic(
                topic_config.get("name"),
                topic_config.get("num_partitions"),
                topic_config.get("replication_factor"),
                topic_config.get("config"),
            )
        )
    editable_topics = topic_controller.filter_existing_topics(topics)
    topics_to_be_changed = [
        topic for topic in editable_topics if topic.config_diff() != {}
    ]
    topic_config_diffs = {
        topic.name: topic.config_diff() for topic in topics_to_be_changed
    }

    if len(topic_config_diffs) > 0:
        click.echo(pretty_topic_diffs(topic_config_diffs))
        if ensure_approval(
            "Are you sure to change configs?", no_verify=state.no_verify
        ):
            topic_controller.alter_configs(topics_to_be_changed)
            click.echo(
                click.style(
                    pretty(
                        {
                            "Successfully changed topics": [
                                topic.name for topic in topics_to_be_changed
                            ]
                        }
                    ),
                    fg="green",
                )
            )
    else:
        click.echo("No topics to edit.")

    new_topics = [topic for topic in topics if topic not in editable_topics]
    if len(new_topics) > 0:
        click.echo(get_output_new_topics(new_topics))
        if ensure_approval(
            "Are you sure to create the new topics?", no_verify=state.no_verify
        ):
            topic_controller.create_topics(new_topics)
            click.echo(
                click.style(
                    pretty(
                        {
                            "Successfully created topics": [
                                topic.name for topic in new_topics
                            ]
                        }
                    ),
                    fg="green",
                )
            )
    else:
        click.echo("No new topics to create.")


@delete.command("topic")
@click.argument(
    "topic-name", required=True, type=click.STRING, autocompletion=list_topics
)
@no_verify_option
@pass_state
def delete_topic(state: State, topic_name: str):
    topic_controller = TopicController(state.cluster)
    if ensure_approval("Are you sure?", no_verify=state.no_verify):
        topic_controller.delete_topic(topic_controller.get_topic(topic_name))

        assert topic_name not in topic_controller.list_topics()


@describe.command("topic")
@click.argument(
    "topic-name", required=True, type=click.STRING, autocompletion=list_topics
)
@pass_state
def describe_topic(state, topic_name):
    partitions, config = TopicController(state.cluster).get_topic(topic_name).describe()

    click.echo(bold(f"Topic: {topic_name}"))

    for idx, partition in enumerate(partitions):
        click.echo(pretty(partition, break_lists=True))

    click.echo(pretty(config))


@get.command("offsets")
@click.argument(
    "topic-name", required=False, type=click.STRING, autocompletion=list_topics
)
@pass_state
def get_offsets(state, topic_name):
    # TODO: Gathering of all offsets takes super long
    topics = TopicController(state.cluster).list_topics(search_string=topic_name)

    offsets = {
        topic.name: max([v for v in topic.get_offsets().values()]) for topic in topics
    }

    click.echo(pretty(offsets))


@describe.command("broker")
@click.argument("broker-id", required=True)
@pass_state
def describe_broker(state, broker_id):
    broker = Broker(state.cluster, broker_id).describe()
    click.echo(pretty(broker, break_lists=True))


@describe.command("consumergroup")
@click.argument("consumer-id", required=False)
@click.option(
    "-v", "--verbose", help="More detailed information.", default=False, is_flag=True
)
@pass_state
def describe_consumergroup(state, consumer_id, verbose):
    try:
        consumer_group = ConsumerGroupController(state.cluster).get_consumergroup(
            consumer_id
        )
        consumer_group_desc = consumer_group.describe(verbose=verbose)

        click.echo(pretty(consumer_group_desc, break_lists=True))
    except ConsumerGroupDoesNotExistException:
        click.echo(bold(f"Consumer Group {consumer_id} not found."))


@get.command("brokers")
@pass_state
def get_brokers(state):
    brokers = state.cluster.brokers
    for broker in brokers:
        click.echo(f"{broker['id']}: {broker['host']}:{broker['port']}")


@get.command("consumergroups")
@pass_state
def get_consumergroups(state):
    groups = ConsumerGroupController(state.cluster).list_consumer_groups()
    for group in groups:
        click.echo(group)


@get.command("topics")
@click.argument("topic", required=False, type=click.STRING, autocompletion=list_topics)
@pass_state
def get_topics(state, topic, o):
    topics = TopicController(state.cluster).list_topics(search_string=topic)
    for topic in topics:
        click.echo(topic.name)


@esque.command(
    "transfer", help="Transfer messages of a topic from one environment to another."
)
@click.argument("topic", required=True)
@click.option(
    "-f",
    "--from",
    "from_context",
    help="Source Context",
    type=click.STRING,
    required=True,
)
@click.option(
    "-t",
    "--to",
    "to_context",
    help="Destination context",
    type=click.STRING,
    required=True,
)
@click.option(
    "-n", "--numbers", help="Number of messages", type=click.INT, required=True
)
@click.option("--last/--first", default=False)
@click.option(
    "-a",
    "--avro",
    help="Set this flag if the topic contains avro data",
    default=False,
    is_flag=True,
)
@pass_state
def transfer(
    state: State,
    topic: str,
    from_context: str,
    to_context: str,
    numbers: int,
    last: bool,
    avro: bool,
):
    current_timestamp_milliseconds = int(round(time.time() * 1000))
    temp_name = topic + "_" + str(current_timestamp_milliseconds)
    group_id = "group_for_" + temp_name
    directory_name = "temp-file_" + temp_name
    base_dir = Path(directory_name)
    state.config.context_switch(from_context)

    with DeleteOnException(base_dir) as working_dir:
        click.echo("\nStart consuming from source context " + blue_bold(from_context))

        if avro:
            consumer = AvroFileConsumer.create(group_id, topic, working_dir, last)
        else:
            consumer = FileConsumer.create(group_id, topic, working_dir, last)

        number_consumed_messages = consumer.consume_to_file(int(numbers))
        click.echo(
            blue_bold(str(number_consumed_messages))
            + " messages consumed successfully."
        )
        click.echo(
            "\nReady to produce to context "
            + blue_bold(to_context)
            + " and target topic "
            + blue_bold(topic)
        )

        if ensure_approval("Do you want to proceed?\n", no_verify=state.no_verify):
            state.config.context_switch(to_context)

            if avro:
                producer = AvroFileProducer.create(working_dir)
            else:
                producer = FileProducer.create(working_dir)

            number_produced_messages = producer.produce_from_file(
                "test_write_from_file_target2"
            )
            click.echo(
                green_bold(str(number_produced_messages))
                + " messages successfully produced to context "
                + green_bold(to_context)
                + " and topic "
                + green_bold(topic)
                + "."
            )

    if base_dir.exists():
        shutil.rmtree(base_dir)


@esque.command("ping", help="Tests the connection to the kafka cluster.")
@click.option("-t", "--times", help="Number of pings.", default=10)
@click.option("-w", "--wait", help="Seconds to wait between pings.", default=1)
@pass_state
def ping(state, times, wait):
    topic_controller = TopicController(state.cluster)
    deltas = []
    try:
        try:
            topic_controller.create_topics([topic_controller.get_topic(PING_TOPIC)])
        except TopicAlreadyExistsException:
            click.echo("Topic already exists.")

        producer = PingProducer()
        consumer = PingConsumer(PING_GROUP_ID, PING_TOPIC, True)

        click.echo(f"Ping with {state.cluster.bootstrap_servers}")

        for i in range(times):
            producer.produce_ping(PING_TOPIC)
            _, delta = consumer.consume_ping()
            deltas.append(delta)
            click.echo(f"m_seq={i} time={delta:.2f}ms")
            sleep(wait)
    except KeyboardInterrupt:
        pass
    finally:
        topic_controller.delete_topic(topic_controller.get_topic(PING_TOPIC))
        click.echo("--- statistics ---")
        click.echo(f"{len(deltas)} messages sent/received")
        click.echo(
            f"min/avg/max = {min(deltas):.2f}/{(sum(deltas)/len(deltas)):.2f}/{max(deltas):.2f} ms"
        )
