import datetime
import time
import uuid
from time import sleep
from typing import Callable, List

import click

from esque.cli.helpers import ensure_approval
from esque.cli.options import State, default_options
from esque.config import PING_TOPIC
from esque.io.handlers import KafkaHandler
from esque.io.handlers.kafka import KafkaHandlerConfig
from esque.io.messages import BinaryMessage
from esque.io.stream_decorators import skip_stream_events
from esque.resources.topic import Topic


@click.command("ping")
@click.option("-t", "--times", help="Number of pings.", default=10)
@click.option("-w", "--wait", help="Seconds to wait between pings.", default=1)
@default_options
def ping(state: State, times: int, wait: int):
    """Test the connection to the kafka cluster.

    Ping the kafka cluster by writing messages to and reading messages from it.
    After the specified number of "pings", return the minimum, maximum, and average time for the round trip.

    \b
    The abbreviations in the output have the following meaning:
        c2s: client to server (time of creation till kafka wrote it to disk)
        s2c: server to client (time from kafka write to disk till client received it again)
        c2c: client to client (complete round trip)
    """
    topic_controller = state.cluster.topic_controller

    if not topic_controller.topic_exists(PING_TOPIC):
        if ensure_approval(
            f"Topic {PING_TOPIC!r} does not exist, do you want to create it?", no_verify=state.no_verify
        ):
            topic_config = {
                "cleanup.policy": "compact,delete",
                "retention.ms": int(datetime.timedelta(days=1).microseconds / 1000),
                "message.timestamp.type": "LogAppendTime",
            }
            topic_controller.create_topics([Topic(PING_TOPIC, num_partitions=10, config=topic_config)])
        else:
            click.echo(click.style("Aborted!", bg="red"))
            return

    ping_id = uuid.uuid4().bytes

    click.echo("Initializing producer.")
    output_handler = KafkaHandler(
        KafkaHandlerConfig(scheme="kafka", host=state.config.current_context, path=PING_TOPIC)
    )
    output_handler.write_message(create_tombstone_message(ping_id))

    input_handler = KafkaHandler(
        KafkaHandlerConfig(scheme="kafka", host=state.config.current_context, path=PING_TOPIC)
    )
    input_stream = filter(key_matches(ping_id), skip_stream_events(input_handler.message_stream()))
    message_iterator = iter(input_stream)

    click.echo("Initializing consumer.")
    input_handler.seek(KafkaHandler.OFFSET_AT_LAST_MESSAGE)
    next(message_iterator)

    click.echo(f"Pinging cluster with bootstrap servers {state.cluster.bootstrap_servers}.")
    deltas = []
    try:
        for i in range(times):
            output_handler.write_message(create_ping_message(ping_id))
            msg_recieved = next(message_iterator)

            dt_created = dt_from_bytes(msg_recieved.value)
            dt_delivered = msg_recieved.timestamp
            dt_received = datetime.datetime.now(tz=datetime.timezone.utc)

            time_client_to_server_ms = (dt_delivered - dt_created).microseconds / 1000
            time_server_to_client_ms = (dt_received - dt_delivered).microseconds / 1000
            time_client_to_client_ms = (dt_received - dt_created).microseconds / 1000
            deltas.append((time_client_to_server_ms, time_server_to_client_ms, time_client_to_client_ms))
            click.echo(
                f"m_seq={i} c2s={time_client_to_server_ms:.2f}ms "
                f"s2c={time_server_to_client_ms:.2f}ms "
                f"c2c={time_client_to_client_ms:.2f}ms"
            )
            sleep(wait)
    except KeyboardInterrupt:
        return

    # make sure our ping messages get cleaned up
    output_handler.write_message(create_tombstone_message(ping_id))

    click.echo("--- statistics ---")
    click.echo(f"{len(deltas)} messages sent/received.")
    c2s_times, s2c_times, c2c_times = zip(*deltas)
    click.echo(f"c2s {stats(c2s_times)}")
    click.echo(f"s2c {stats(s2c_times)}")
    click.echo(f"c2c {stats(c2c_times)}")


def key_matches(ping_id: bytes) -> Callable[[BinaryMessage], bool]:
    def matcher(msg: BinaryMessage) -> bool:
        return msg.key == ping_id

    return matcher


def stats(deltas: List[int]) -> str:
    return f"min/avg/max = {min(deltas):.2f}/{(sum(deltas) / len(deltas)):.2f}/{max(deltas):.2f} ms"


def create_ping_message(ping_id) -> BinaryMessage:
    create_time = datetime.datetime.fromtimestamp(round(time.time(), 3))
    return BinaryMessage(
        key=ping_id, value=dt_to_bytes(create_time), partition=-1, offset=-1, timestamp=create_time, headers=[]
    )


def create_tombstone_message(ping_id) -> BinaryMessage:
    create_time = datetime.datetime.fromtimestamp(round(time.time(), 3))
    return BinaryMessage(key=ping_id, value=None, partition=-1, offset=-1, timestamp=create_time, headers=[])


def dt_to_bytes(dt: datetime.datetime) -> bytes:
    return int(dt.timestamp() * 1000).to_bytes(8, "big")


def dt_from_bytes(data: bytes) -> datetime.datetime:
    return datetime.datetime.fromtimestamp(int.from_bytes(data, "big") / 1000, tz=datetime.timezone.utc)
