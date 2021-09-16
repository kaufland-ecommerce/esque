from time import sleep

import click

from esque.cli.options import State, default_options
from esque.clients.consumer import ConsumerFactory
from esque.clients.producer import PingProducer
from esque.config import ESQUE_GROUP_ID, PING_TOPIC
from esque.errors import TopicAlreadyExistsException
from esque.resources.topic import Topic


@click.command("ping")
@click.option("-t", "--times", help="Number of pings.", default=10)
@click.option("-w", "--wait", help="Seconds to wait between pings.", default=1)
@default_options
def ping(state: State, times: int, wait: int):
    """Test the connection to the kafka cluster.

    Ping the kafka cluster by writing messages to and reading messages from it.
    After the specified number of "pings", return the minimum, maximum, and average time for the round trip.
    """
    topic_controller = state.cluster.topic_controller
    deltas = []
    try:
        try:
            topic_controller.create_topics([Topic(PING_TOPIC)])
        except TopicAlreadyExistsException:
            pass
        producer = PingProducer(PING_TOPIC)
        consumer = ConsumerFactory().create_ping_consumer(group_id=ESQUE_GROUP_ID, topic_name=PING_TOPIC)
        click.echo(f"Pinging with {state.cluster.bootstrap_servers}.")

        for i in range(times):
            producer.produce()
            _, delta = consumer.consume()
            deltas.append(delta)
            click.echo(f"m_seq={i} time={delta:.2f}ms")
            sleep(wait)
    except KeyboardInterrupt:
        return
    topic_controller.delete_topic(Topic(PING_TOPIC))
    click.echo("--- statistics ---")
    click.echo(f"{len(deltas)} messages sent/received.")
    click.echo(f"min/avg/max = {min(deltas):.2f}/{(sum(deltas) / len(deltas)):.2f}/{max(deltas):.2f} ms")
