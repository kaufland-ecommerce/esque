from time import sleep

import click
from click import version_option

from esque.__version__ import __version__
from esque.broker import Broker
from esque.cli.helpers import ensure_approval
from esque.cli.options import State, no_verify_option, pass_state
from esque.cli.output import bold, pretty
from esque.clients import Consumer, Producer
from esque.cluster import Cluster
from esque.config import PING_TOPIC, Config
from esque.consumergroup import ConsumerGroupController
from esque.errors import (
    ConsumerGroupDoesNotExistException,
    ContextNotDefinedException,
    TopicAlreadyExistsException,
)
from esque.topic import TopicController


@click.group(help="(Kafka-)esque.")
@version_option(__version__)
def esque():
    pass


@esque.group(help="Get a quick overview of different resources.")
def get():
    pass


@esque.group(help="Get detailed informations about a resource.")
def describe():
    pass


@esque.group(help="Create a new instance of a resource.")
def create():
    pass


@esque.group(help="Delete a resource.")
def delete():
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
def create_topic(state: State, topic_name):
    if ensure_approval("Are you sure?", no_verify=state.no_verify):
        TopicController(state.cluster).create_topic(topic_name)


@delete.command("topic")
@click.argument(
    "topic-name", required=True, type=click.STRING, autocompletion=list_topics
)
@no_verify_option
@pass_state
def delete_topic(state: State, topic_name: str):
    topic_controller = TopicController(state.cluster)
    if ensure_approval("Are you sure?", no_verify=state.no_verify):
        topic_controller.delete_topic(topic_name)

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
def get_topics(state, topic):
    topics = TopicController(state.cluster).list_topics(search_string=topic)
    for topic in topics:
        click.echo(topic.name)


@esque.command("ping", help="Tests the connection to the kafka cluster.")
@click.option("-t", "--times", help="Number of pings.", default=10)
@click.option("-w", "--wait", help="Seconds to wait between pings.", default=1)
@pass_state
def ping(state, times, wait):
    topic_controller = TopicController(state.cluster)
    deltas = []
    try:
        try:
            topic_controller.create_topic(PING_TOPIC)
        except TopicAlreadyExistsException:
            click.echo("Topic already exists.")

        producer = Producer()
        consumer = Consumer()

        click.echo(f"Ping with {state.cluster.bootstrap_servers}")

        for i in range(times):
            producer.produce_ping()
            _, delta = consumer.consume_ping()
            deltas.append(delta)
            click.echo(f"m_seq={i} time={delta:.2f}ms")
            sleep(wait)
    except KeyboardInterrupt:
        pass
    finally:
        topic_controller.delete_topic(PING_TOPIC)
        click.echo("--- statistics ---")
        click.echo(f"{len(deltas)} messages sent/received")
        click.echo(
            f"min/avg/max = {min(deltas):.2f}/{(sum(deltas)/len(deltas)):.2f}/{max(deltas):.2f} ms"
        )
