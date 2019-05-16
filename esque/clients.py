from typing import Optional, Tuple

import click
import confluent_kafka
import pendulum
from confluent_kafka import TopicPartition

from esque.config import Config, PING_GROUP_ID, PING_TOPIC
from esque.errors import raise_for_kafka_error, raise_for_message
from esque.helpers import delivery_callback, delta_t

DEFAULT_RETENTION_MS = 7 * 24 * 60 * 60 * 1000


class Consumer:
    def __init__(self):
        self._config = Config().create_confluent_config()
        self._config.update(
            {
                "group.id": PING_GROUP_ID,
                "error_cb": raise_for_kafka_error,
                # We need to commit offsets manually once we"re sure it got saved
                # to the sink
                "enable.auto.commit": True,
                "enable.partition.eof": False,
                # We need this to start at the last committed offset instead of the
                # latest when subscribing for the first time
                "default.topic.config": {"auto.offset.reset": "latest"},
            }
        )
        self._consumer = confluent_kafka.Consumer(self._config)
        self._assign_exact_partitions(PING_TOPIC)

    def consume_ping(self) -> Optional[Tuple[str, int]]:
        msg = self._consumer.consume(timeout=10)[0]

        raise_for_message(msg)

        msg_sent_at = pendulum.from_timestamp(float(msg.value()))
        delta_sent = pendulum.now() - msg_sent_at
        return msg.key(), delta_sent.microseconds / 1000

    def _assign_exact_partitions(self, topic: str) -> None:
        self._consumer.assign([TopicPartition(topic=topic, partition=0, offset=0)])


class Producer:
    def __init__(self):
        self._config = Config().create_confluent_config()
        self._config.update(
            {"on_delivery": delivery_callback, "error_cb": raise_for_kafka_error}
        )
        self._producer = confluent_kafka.Producer(self._config)

    def produce_ping(self):
        start = pendulum.now()
        self._producer.produce(
            topic=PING_TOPIC, key=str(0), value=str(pendulum.now().timestamp())
        )
        while True:
            left_messages = self._producer.flush(1)
            if left_messages == 0:
                break
            click.echo(
                f"{delta_t(start)} | Still {left_messages} messages left, flushing..."
            )
