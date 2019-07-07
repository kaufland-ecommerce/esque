import pathlib
import pickle
from typing import Optional, Tuple

import click
import confluent_kafka
import pendulum
from confluent_kafka import TopicPartition, Message

from esque.config import Config
from esque.errors import raise_for_kafka_error, raise_for_message, KafkaException
from esque.helpers import delivery_callback, delta_t
from esque.message import Serializer

DEFAULT_RETENTION_MS = 7 * 24 * 60 * 60 * 1000


class Consumer:
    def __init__(self, group_id: str, topic_name: str, last: bool):
        offset_reset = "earliest"
        if last:
            offset_reset = "latest"

        self._config = Config().create_confluent_config()
        self._config.update(
            {
                "group.id": group_id,
                "error_cb": raise_for_kafka_error,
                # We need to commit offsets manually once we"re sure it got saved
                # to the sink
                "enable.auto.commit": True,
                "enable.partition.eof": False,
                # We need this to start at the last committed offset instead of the
                # latest when subscribing for the first time
                "default.topic.config": {"auto.offset.reset": offset_reset},
            }
        )
        self._consumer = confluent_kafka.Consumer(self._config)
        self._assign_exact_partitions(topic_name)

    def consume_ping(self) -> Optional[Tuple[str, int]]:
        msg = self._consumer.consume(timeout=10)[0]

        raise_for_message(msg)

        msg_sent_at = pendulum.from_timestamp(float(msg.value()))
        delta_sent = pendulum.now() - msg_sent_at
        return msg.key(), delta_sent.microseconds / 1000

    def consume_to_file(self, serializer: Serializer, amount: int) -> int:
        counter = 0
        with (serializer.get_working_directory_path() / "data").open("wb") as file:
            while counter < amount:
                try:
                    message = self._consume_single_message()
                except KafkaException as ex:
                    print("An error occurred: " + ex.message)
                    continue

                serializer.serialize(message, file)
                counter += 1

        return counter

    def _consume_single_message(self) -> Message:
        poll_limit = 10
        counter = 0
        try:
            while counter < poll_limit:
                message = self._consumer.poll(timeout=10)
                if message is None:
                    counter += 1
                    continue
                raise_for_message(message)
                return message
        except KafkaException as ex:
            print("An error occurred: " + ex.message)


    def _assign_exact_partitions(self, topic: str) -> None:
        self._consumer.assign([TopicPartition(topic=topic, partition=0, offset=0)])


class Producer:
    def __init__(self):
        self._config = Config().create_confluent_config()
        self._config.update(
            {"on_delivery": delivery_callback, "error_cb": raise_for_kafka_error}
        )
        self._producer = confluent_kafka.Producer(self._config)

    def produce_ping(self, topic_name: str):
        start = pendulum.now()
        self._producer.produce(
            topic=topic_name, key=str(0), value=str(pendulum.now().timestamp())
        )
        while True:
            left_messages = self._producer.flush(1)
            if left_messages == 0:
                break
            click.echo(
                f"{delta_t(start)} | Still {left_messages} messages left, flushing..."
            )

    def produce_from_file(self, file_path: str, topic_name: str) -> int:
        with open(file_path, "rb") as file:
            counter = 0
            while True:
                try:
                    record = pickle.load(file)
                except EOFError:
                    break

                self._producer.produce(
                    topic=topic_name, key=record["key"], value=record["value"]
                )
                counter += 1

            while True:
                left_messages = self._producer.flush(1)
                if left_messages == 0:
                    break

            return counter
